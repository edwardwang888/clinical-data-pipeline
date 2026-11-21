"""Shared fixtures and helpers.

Tests that need Redis use, in order of preference:
1. TEST_REDIS_URL if set (`make test` runs the tests in Docker against the compose Redis),
2. a throwaway `redis-server` started here if one is installed,
3. otherwise they are skipped with a message saying how to run them.
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import redis

REPO_ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url(tmp_path_factory):
    url = os.environ.get("TEST_REDIS_URL")
    if url:
        yield url
        return

    exe = shutil.which("redis-server")
    if not exe:
        pytest.skip("needs Redis: run `make test` (Docker), install redis-server, or set TEST_REDIS_URL")

    port = free_port()
    workdir = tmp_path_factory.mktemp("redis")
    proc = subprocess.Popen(
        [exe, "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--dir", str(workdir)],
        stdout=subprocess.DEVNULL,
    )
    url = f"redis://127.0.0.1:{port}/0"
    client = redis.Redis.from_url(url)
    deadline = time.monotonic() + 10
    while True:
        try:
            client.ping()
            break
        except redis.ConnectionError:
            if time.monotonic() > deadline:
                proc.kill()
                raise
            time.sleep(0.05)
    yield url
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def redis_client(redis_url):
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def stream_name(redis_client):
    """A stream name unique to this test, deleted afterwards (Redis may be shared)."""
    name = f"test.hl7.raw.{uuid.uuid4().hex[:8]}"
    yield name
    redis_client.delete(name)


class ListenerProcess:
    """The real listener (`python -m services.listener`) as a child process, writing to
    `data_dir`/raw and `data_dir`/rejected, with its log in `data_dir`/listener.log."""

    def __init__(self, port, data_dir, redis_url, stream, extra_env=None):
        self.port = port
        self.raw_dir = data_dir / "raw"
        self.rejected_dir = data_dir / "rejected"
        self.log_path = data_dir / "listener.log"
        self.env = {
            **os.environ,
            "LISTENER_HOST": "127.0.0.1",
            "LISTENER_PORT": str(port),
            "RAW_DIR": str(self.raw_dir),
            "RAW_REJECTED_DIR": str(self.rejected_dir),
            "REDIS_URL": redis_url,
            "RAW_STREAM": stream,
            "DLQ_STREAM": f"{stream}.dlq",
            "LOG_LEVEL": "WARNING",
            **(extra_env or {}),
        }
        self.proc = None
        self.starts = 0

    def start(self):
        with open(self.log_path, "a") as log:
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "services.listener"],
                cwd=REPO_ROOT,
                env=self.env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        self.starts += 1
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"listener exited early:\n{self.log_path.read_text()}")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("listener did not start listening in time")

    def stop(self, sig=signal.SIGTERM):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(timeout=15)
