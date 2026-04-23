"""SSH helpers for the Catena client."""

from __future__ import annotations

import shlex
import subprocess

from catena_common.jsonio import load_json_text


class SSHError(RuntimeError):
    """Raised when SSH itself fails before a usable remote JSON response."""


class RemoteResponseError(ValueError):
    """Raised when the remote command output cannot be parsed as expected."""


def run_ssh_command(
    host: str,
    user: str,
    remote_args: list[str],
    stdin_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a Catena command on the remote host over SSH."""

    return subprocess.run(
        ["ssh", f"{user}@{host}", " ".join(shlex.quote(arg) for arg in remote_args)],
        input=stdin_text,
        capture_output=True,
        text=True,
        check=False,
    )


def parse_json_response(text: str) -> dict[str, object]:
    """Parse a JSON response returned by the remote server."""

    parsed = load_json_text(text)
    if not isinstance(parsed, dict):
        msg = "remote command returned non-object JSON"
        raise ValueError(msg)
    return parsed


def handle_remote_result(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    """Validate the remote command output and surface useful SSH errors."""

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    if stdout:
        try:
            return parse_json_response(stdout)
        except ValueError as exc:
            msg = f"remote command returned invalid JSON: {exc}"
            if stderr:
                msg = f"{msg}; stderr: {stderr}"
            raise RemoteResponseError(msg) from exc

    if result.returncode != 0:
        msg = f"ssh failed with exit code {result.returncode}"
        if stderr:
            msg = f"{msg}: {stderr}"
        raise SSHError(msg)

    msg = "remote command returned empty output"
    if stderr:
        msg = f"{msg}: {stderr}"
    raise RemoteResponseError(msg)
