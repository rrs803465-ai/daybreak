import asyncio
import logging
import os
import socket
import struct
import sys
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

PORT = int(os.environ.get("PORT", 8080))

# Binary Protocol Constants
CMD_CONNECT   = 0x01
CMD_DATA      = 0x02
CMD_CLOSE     = 0x03
CMD_CONNECTED = 0x04
CMD_ERROR     = 0x05
CMD_PING      = 0x06
CMD_PONG      = 0x07

HEADER_FORMAT = ">IBI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

class ResilientMultiplexedServer:
    def __init__(self):
        # Maps stream_id -> (asyncio.StreamReader, asyncio.StreamWriter)
        self.streams = {}
        self.lock = asyncio.Lock()

    async def handle_websocket(self, ws):
        logging.info("New persistent WSS tunnel established from client.")
        
        try:
            async for message in ws:
                if not isinstance(message, bytes) or len(message) < HEADER_SIZE:
                    continue
                
                stream_id, cmd, payload_len = struct.unpack(
                    HEADER_FORMAT, message[:HEADER_SIZE]
                )
                payload = message[HEADER_SIZE : HEADER_SIZE + payload_len]

                if cmd == CMD_CONNECT:
                    asyncio.create_task(self._on_connect(ws, stream_id, payload))

                elif cmd == CMD_DATA:
                    asyncio.create_task(self._on_data(stream_id, payload))

                elif cmd == CMD_CLOSE:
                    asyncio.create_task(self._close_stream(stream_id))

                elif cmd == CMD_PING:
                    pong_frame = struct.pack(HEADER_FORMAT, 0, CMD_PONG, 0)
                    await ws.send(pong_frame)

        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Client closed WSS tunnel gracefully.")
        except websockets.exceptions.ConnectionClosedError as e:
            logging.warning(f"WSS connection closed with error: {e}")
        except Exception as e:
            logging.error(f"Unexpected WSS error: {e}")
        finally:
            await self._cleanup_all_streams()

    async def _on_connect(self, ws, stream_id, payload):
        if len(payload) < 3:
            return

        try:
            port, host_len = struct.unpack(">HB", payload[:3])
            host = payload[3 : 3 + host_len].decode("utf-8", errors="ignore")

            # Non-blocking TCP connection to target destination
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10.0
            )

            # Apply OS-level TCP tuning
            sock = writer.get_extra_info("socket")
            if sock:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

            async with self.lock:
                self.streams[stream_id] = (reader, writer)

            # Notify client that connection to remote host succeeded
            ack_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CONNECTED, 0)
            await ws.send(ack_frame)

            # Start reading from target server and pumping back to WSS
            asyncio.create_task(self._pump_target_to_ws(ws, stream_id, reader))

        except Exception as e:
            err_bytes = str(e).encode("utf-8")
            err_frame = (
                struct.pack(HEADER_FORMAT, stream_id, CMD_ERROR, len(err_bytes))
                + err_bytes
            )
            try:
                await ws.send(err_frame)
            except Exception:
                pass

    async def _on_data(self, stream_id, payload):
        async with self.lock:
            stream = self.streams.get(stream_id)

        if stream:
            _, writer = stream
            try:
                writer.write(payload)
                await writer.drain()
            except Exception:
                await self._close_stream(stream_id)

    async def _pump_target_to_ws(self, ws, stream_id, reader):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                header = struct.pack(HEADER_FORMAT, stream_id, CMD_DATA, len(data))
                await ws.send(header + data)
        except Exception:
            pass
        finally:
            close_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CLOSE, 0)
            try:
                await ws.send(close_frame)
            except Exception:
                pass
            await self._close_stream(stream_id)

    async def _close_stream(self, stream_id):
        async with self.lock:
            stream = self.streams.pop(stream_id, None)

        if stream:
            _, writer = stream
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _cleanup_all_streams(self):
        async with self.lock:
            stream_ids = list(self.streams.keys())

        for sid in stream_ids:
            await self._close_stream(sid)


async def main():
    server = ResilientMultiplexedServer()
    async with websockets.serve(
        server.handle_websocket,
        "0.0.0.0",
        PORT,
        ping_interval=12,
        ping_timeout=30,
        max_size=None,
        write_limit=2097152
    ):
        logging.info(f"High-Resilience Relay Engine active on port {PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server terminated.")
