"""The deployed-stack launcher must fail closed when credential lookup fails.

Both CLIs are stubs: these tests never contact AWS or start Docker.
"""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "local_app" / "run_against_aws.sh"


@pytest.mark.parametrize("key_lookup", ["exit 42", "echo None", "echo ''"])
def test_failed_api_key_lookup_does_not_start_containers(tmp_path, key_lookup):
    aws = tmp_path / "aws"
    aws.write_text(
        f'#!/bin/sh\nif [ "$1" = apigateway ]; then\n  {key_lookup}\nelse\n  echo test-output\nfi\n'
    )
    aws.chmod(0o755)
    called = tmp_path / "docker-called"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\ntouch "$DOCKER_CALLED_FILE"\n')
    docker.chmod(0o755)
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "AWS_REGION": "us-east-1",
            "DOCKER_CALLED_FILE": str(called),
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert not called.exists()
