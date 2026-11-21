"""Run the MLLP listener:  python -m services.listener

Configuration (environment variables):
    LISTENER_HOST       interface to bind (default 0.0.0.0)
    LISTENER_PORT       port to listen on (default 2575)
    RAW_DIR             raw archive folder (default data/raw)
    RAW_REJECTED_DIR    evidence for rejected (AR) messages (default data/rejected)
    REDIS_URL           Redis to publish events to (default redis://localhost:6379/0)
    RAW_STREAM          stream name (default hl7.raw)
    MAX_MESSAGE_BYTES   largest accepted frame (default 16 MiB)
    LOG_LEVEL           default INFO
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from hl7.mllp import start_hl7_server

from pipeline.publisher import RAW_STREAM, RedisStreamPublisher
from pipeline.raw_store import LocalRawStore
from services.listener.server import Listener

log = logging.getLogger("services.listener")


async def serve() -> None:
    host = os.environ.get("LISTENER_HOST", "0.0.0.0")
    port = int(os.environ.get("LISTENER_PORT", "2575"))
    raw_dir = os.environ.get("RAW_DIR", "data/raw")
    rejected_dir = os.environ.get("RAW_REJECTED_DIR", "data/rejected")
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    stream = os.environ.get("RAW_STREAM", RAW_STREAM)
    max_bytes = int(os.environ.get("MAX_MESSAGE_BYTES", str(16 * 1024 * 1024)))

    publisher = RedisStreamPublisher.from_url(redis_url, stream)
    try:
        publisher.client.ping()
    except Exception as e:
        # Keep running: until Redis is back every message gets AE and is resent.
        log.warning("Redis not reachable at startup (%s); messages will get AE until it is", e)

    listener = Listener(LocalRawStore(raw_dir), publisher, LocalRawStore(rejected_dir))
    server = await start_hl7_server(listener.handle_connection, host, port, limit=max_bytes)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    log.info("listening on %s:%d raw_dir=%s rejected_dir=%s stream=%s", host, port, raw_dir, rejected_dir, stream)
    await stop.wait()

    log.info("shutting down")
    server.close()
    await listener.close_connections()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
