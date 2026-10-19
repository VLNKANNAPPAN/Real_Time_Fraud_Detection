"""Capture the live Streamlit dashboard through Edge's DevTools protocol."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
import urllib.request
from pathlib import Path

import websockets


def target(debug_port: int, timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{debug_port}/json", timeout=2
            ) as response:
                pages = json.load(response)
            page = next(
                item
                for item in pages
                if item.get("type") == "page" and item.get("url", "").startswith("http")
            )
            return page
        except (OSError, StopIteration, json.JSONDecodeError):
            time.sleep(0.5)
    raise TimeoutError("Edge DevTools page did not become available")


async def capture(websocket_url: str, output: Path, settle_seconds: float) -> None:
    async with websockets.connect(websocket_url, max_size=20_000_000) as socket:
        request_id = 0

        async def command(method: str, params: dict | None = None) -> dict:
            nonlocal request_id
            request_id += 1
            wanted = request_id
            await socket.send(json.dumps({"id": wanted, "method": method, "params": params or {}}))
            while True:
                message = json.loads(await socket.recv())
                if message.get("id") == wanted:
                    if "error" in message:
                        raise RuntimeError(message["error"])
                    return message.get("result", {})

        await command("Page.enable")
        await command(
            "Emulation.setDeviceMetricsOverride",
            {"width": 1600, "height": 1100, "deviceScaleFactor": 1, "mobile": False},
        )
        await asyncio.sleep(settle_seconds)
        result = await command(
            "Page.captureScreenshot",
            {"format": "png", "captureBeyondViewport": False, "fromSurface": True},
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(base64.b64decode(result["data"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-port", type=int, default=9223)
    parser.add_argument("--output", type=Path, default=Path("demo/dashboard.png"))
    parser.add_argument("--settle-seconds", type=float, default=12.0)
    args = parser.parse_args()
    page = target(args.debug_port)
    asyncio.run(capture(page["webSocketDebuggerUrl"], args.output, args.settle_seconds))
    print(args.output.resolve())


if __name__ == "__main__":
    main()
