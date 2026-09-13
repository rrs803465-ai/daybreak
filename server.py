# ==============================================================================
# server.py - Enterprise Resilient WSS Multiplexing Relay Engine
# Target: Maximum Concurrency, Zero Socket Drift, Backpressure Management
# ==============================================================================

import asyncio
import logging
import os
import socket
import struct
import sys
import time
from typing import Dict, Tuple, Optional

# Attempt to load uvloop for high-performance event loop execution
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import websockets
from websockets.server import WebSocketServerProtocol

# Logging Infrastructure
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] [SERVER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Global Configuration & Environment Flags
PORT = int(os.environ.get("PORT", 8080))
MAX_WRITE_BUFFER = 8 * 1024 * 1024  # 8 MB per WS frame buffer
READ_CHUNK_SIZE = 64 * 1024        # 64 KB per TCP read call

# Binary Protocol Frame Layout (9-byte Header)
# [Stream ID: uint32 (4B)] [CMD: uint8 (1B)] [Payload Length: uint32 (4B)]
HEADER_FORMAT = ">IBI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Command Identifiers
CMD_CONNECT   = 0x01  # Client -> Server: Initiate TCP bridge
CMD_DATA      = 0x02  # Bidirectional: Stream payload
CMD_CLOSE     = 0x03  # Bidirectional: Tear down stream
CMD_CONNECTED = 0x04  # Server -> Client: TCP target reachability ACK
CMD_ERROR     = 0x05  # Server -> Client: Fault notification
CMD_PING      = 0x06  # Client -> Server: Heartbeat ping
CMD_PONG      = 0x07  # Server -> Client: Heartbeat pong
CMD_THROTTLE  = 0x08  # Backpressure signal: Pause target read stream
CMD_RESUME    = 0x09  # Backpressure signal: Resume target read stream


class ServerPerformanceMonitor:
    def __init__(self):
        self.bytes_in = 0
        self.bytes_out = 0
        self.total_streams_opened = 0
        self.active_streams = 0
        self.start_timestamp = time.time()

    def generate_telemetry_report(self) -> str:
        elapsed = max(1.0, time.time() - self.start_timestamp)
        rx_mb = self.bytes_in / (1024 * 1024)
        tx_mb = self.bytes_out / (1024 * 1024)
        return (
            f"Uptime: {int(elapsed)}s | Active Streams: {self.active_streams} | "
            f"Total Streams: {self.total_streams_opened} | "
            f"Ingress: {rx_mb:.2f} MB ({self.bytes_in * 8 / elapsed / 1000000:.2f} Mbps) | "
            f"Egress: {tx_mb:.2f} MB ({self.bytes_out * 8 / elapsed / 1000000:.2f} Mbps)"
        )


telemetry = ServerPerformanceMonitor()


class EnterpriseMultiplexedServer:
    def __init__(self):
        # Stream Registry: stream_id -> (StreamReader, StreamWriter)
        self.active_streams: Dict[int, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        # Pause Flags for Flow Control: stream_id -> asyncio.Event
        self.flow_events: Dict[int, asyncio.Event] = {}
        self.global_registry_lock = asyncio.Lock()

    async def safe_websocket_write(self, ws: WebSocketServerProtocol, lock: asyncio.Lock, frame_data: bytes):
        """Thread-safe, deadlock-free WebSocket frame output pump."""
        async with lock:
            try:
                await ws.send(frame_data)
                telemetry.bytes_out += len(frame_data)
            except websockets.exceptions.ConnectionClosed:
                pass
            except Exception as ex:
                logging.debug(f"Frame dispatch dropped: {ex}")

    async def handle_connection_session(self, ws: WebSocketServerProtocol):
        """Main connection handler managing persistent WebSocket sessions."""
        client_ip = ws.remote_address[0] if ws.remote_address else "unknown"
        logging.info(f"Incoming WSS tunnel connection accepted from {client_ip}")

        write_lock = asyncio.Lock()
        telemetry_task = asyncio.create_task(self._periodic_telemetry_loop())

        try:
            async for raw_message in ws:
                if not isinstance(raw_message, bytes) or len(raw_message) < HEADER_SIZE:
                    continue

                telemetry.bytes_in += len(raw_message)

                # Unpack 9-byte header
                stream_id, command, payload_length = struct.unpack(
                    HEADER_FORMAT, raw_message[:HEADER_SIZE]
                )
                payload = raw_message[HEADER_SIZE : HEADER_SIZE + payload_length]

                if command == CMD_CONNECT:
                    asyncio.create_task(self._execute_target_connect(ws, write_lock, stream_id, payload))

                elif command == CMD_DATA:
                    asyncio.create_task(self._dispatch_payload_to_target(stream_id, payload))

                elif command == CMD_CLOSE:
                    asyncio.create_task(self._terminate_stream_instance(stream_id))

                elif command == CMD_PING:
                    pong_frame = struct.pack(HEADER_FORMAT, 0, CMD_PONG, 0)
                    asyncio.create_task(self.safe_websocket_write(ws, write_lock, pong_frame))

                elif command == CMD_THROTTLE:
                    if stream_id in self.flow_events:
                        self.flow_events[stream_id].clear()

                elif command == CMD_RESUME:
                    if stream_id in self.flow_events:
                        self.flow_events[stream_id].set()

        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Client closed WSS session gracefully.")
        except websockets.exceptions.ConnectionClosedError as e:
            logging.warning(f"WSS session abruptly disconnected: {e}")
        except Exception as err:
            logging.error(f"Fatal error in connection session loop: {err}", exc_info=True)
        finally:
            telemetry_task.cancel()
            await self._purge_all_active_streams()

    async def _execute_target_connect(self, ws: WebSocketServerProtocol, write_lock: asyncio.Lock, stream_id: int, payload: bytes):
        """Connects non-blockingly to target TCP server (e.g. cloudflared port 7844, SSH, HTTP)."""
        if len(payload) < 3:
            return

        try:
            port, host_length = struct.unpack(">HB", payload[:3])
            host = payload[3 : 3 + host_length].decode("utf-8", errors="ignore")

            logging.info(f"[Stream {stream_id}] Connecting to target host -> {host}:{port}")

            # Non-blocking connection with 15s timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=15.0
            )

            # Apply socket-level performance flags
            sock = writer.get_extra_info("socket")
            if sock:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    # Linux-specific TCP keepalive tuning
                    if hasattr(socket, "TCP_KEEPIDLE"):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                    if hasattr(socket, "TCP_KEEPINTVL"):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 3)
                    if hasattr(socket, "TCP_KEEPCNT"):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
                except Exception as sock_err:
                    logging.debug(f"Socket tuning warning: {sock_err}")

            flow_event = asyncio.Event()
            flow_event.set()

            async with self.global_registry_lock:
                self.active_streams[stream_id] = (reader, writer)
                self.flow_events[stream_id] = flow_event
                telemetry.total_streams_opened += 1
                telemetry.active_streams += 1

            # Send back CMD_CONNECTED frame
            ack_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CONNECTED, 0)
            await self.safe_websocket_write(ws, write_lock, ack_frame)

            # Spawn bidirectional read pump from target server to WebSocket
            asyncio.create_task(self._pump_target_to_websocket(ws, write_lock, stream_id, reader))

        except Exception as error:
            logging.error(f"[Stream {stream_id}] Target connection error: {error}")
            err_msg = str(error).encode("utf-8")
            err_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_ERROR, len(err_msg)) + err_msg
            await self.safe_websocket_write(ws, write_lock, err_frame)

    async def _dispatch_payload_to_target(self, stream_id: int, payload: bytes):
        """Dispatches data received from WSS to local TCP target socket."""
        async with self.global_registry_lock:
            stream = self.active_streams.get(stream_id)

        if stream:
            _, writer = stream
            try:
                writer.write(payload)
                await writer.drain()
            except Exception as ex:
                logging.debug(f"[Stream {stream_id}] Write failed: {ex}")
                await self._terminate_stream_instance(stream_id)

    async def _pump_target_to_websocket(self, ws: WebSocketServerProtocol, write_lock: asyncio.Lock, stream_id: int, reader: asyncio.StreamReader):
        """Reads chunks from target TCP connection and frames them into WSS messages."""
        try:
            while True:
                # Wait if backpressure flow control event is cleared
                if stream_id in self.flow_events:
                    await self.flow_events[stream_id].wait()

                data = await reader.read(READ_CHUNK_SIZE)
                if not data:
                    break

                header = struct.pack(HEADER_FORMAT, stream_id, CMD_DATA, len(data))
                await self.safe_websocket_write(ws, write_lock, header + data)

        except Exception as ex:
            logging.debug(f"[Stream {stream_id}] Read pump exception: {ex}")
        finally:
            close_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CLOSE, 0)
            await self.safe_websocket_write(ws, write_lock, close_frame)
            await self._terminate_stream_instance(stream_id)

    async def _terminate_stream_instance(self, stream_id: int):
        """Closes target socket clean-up stream entry safely."""
        async with self.global_registry_lock:
            stream = self.active_streams.pop(stream_id, None)
            self.flow_events.pop(stream_id, None)
            if stream:
                telemetry.active_streams -= 1

        if stream:
            _, writer = stream
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logging.info(f"[Stream {stream_id}] Stream context terminated cleanly.")

    async def _purge_all_active_streams(self):
        """Purges all running stream handlers on WebSocket disconnect."""
        async with self.global_registry_lock:
            all_stream_ids = list(self.active_streams.keys())

        for sid in all_stream_ids:
            await self._terminate_stream_instance(sid)

    async def _periodic_telemetry_loop(self):
        """Logs metrics every 30 seconds."""
        while True:
            await asyncio.sleep(30)
            logging.info(f"TELEMETRY -> {telemetry.generate_telemetry_report()}")


async def main():
    server_engine = EnterpriseMultiplexedServer()
    async with websockets.serve(
        server_engine.handle_connection_session,
        "0.0.0.0",
        PORT,
        ping_interval=10,
        ping_timeout=25,
        max_size=None,
        write_limit=MAX_WRITE_BUFFER
    ):
        logging.info(f"Daybreak Enterprise Relay operational on port {PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down Daybreak Relay Server...")
