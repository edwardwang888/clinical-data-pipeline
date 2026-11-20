"""Send synthetic HL7 messages to the listener:  python -m producer --count 100

Options can also come from the environment (handy in docker-compose):
    LISTENER_HOST, LISTENER_PORT, PRODUCER_COUNT, PRODUCER_RATE, PRODUCER_SEED
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from producer.messages import MessageGenerator
from producer.sender import MllpSender

log = logging.getLogger("producer")


def parse_args() -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=env("LISTENER_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(env("LISTENER_PORT", "2575")))
    p.add_argument("--count", type=int, default=int(env("PRODUCER_COUNT", "0")), help="messages to send; 0 = forever")
    p.add_argument("--rate", type=float, default=float(env("PRODUCER_RATE", "2")), help="messages per second; 0 = as fast as possible")
    p.add_argument("--seed", type=int, default=int(env("PRODUCER_SEED")) if env("PRODUCER_SEED") else None)
    return p.parse_args()


async def run(args: argparse.Namespace) -> None:
    generator = MessageGenerator(seed=args.seed)
    sender = MllpSender(args.host, args.port)
    interval = 1 / args.rate if args.rate > 0 else 0
    log.info("sending to %s:%d run_id=%s count=%s rate=%s/s", args.host, args.port, generator.run_id, args.count or "forever", args.rate)
    sent = 0
    try:
        while not args.count or sent < args.count:
            message = generator.next()
            await sender.send(message.raw, message.control_id)
            sent += 1
            log.info("ACKed %s %s", message.control_id, message.message_type)
            if interval:
                await asyncio.sleep(interval)
    finally:
        await sender.close()
        log.info("done: %d messages acknowledged, %d resends", sent, sender.resends)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
