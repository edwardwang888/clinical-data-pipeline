"""Shared fixtures.

Tests that need Redis use, in order of preference:
1. TEST_REDIS_URL if set (`make test` runs the tests in Docker against the compose Redis),
2. a throwaway `redis-server` started here if one is installed,
3. otherwise they are skipped with a message saying how to run them.
"""

import os
import shutil
import socket
import subprocess
import time
import uuid

import pytest
import redis


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
