#!/usr/bin/env python3
"""Synthetic crawl origins for the scaling benchmark. Standard library only.

Serves ORIGINS origins on consecutive ports from BASE_PORT. An origin is scheme + host + port, so
each port is its own politeness domain to the crawler, which is how a benchmark gets many origins
from one pod. Origin o has PAGES pages in a tree (/p/0 links to /p/1../p/3, and so on); every page
answers after LATENCY_MS, as a remote server would, and has distinct text, so no two are duplicates.
robots.txt allows everything. GET /__stats on any port returns the request counts.
"""

import asyncio
import json
import os
import re

ORIGINS = int(os.environ["ORIGINS"])
BASE_PORT = int(os.environ["BASE_PORT"])
PAGES = int(os.environ["PAGES"])
LATENCY = int(os.environ["LATENCY_MS"]) / 1000
FANOUT = 3
ROBOTS = b"User-agent: *\nAllow: /\n"
TOPICS = [
    "queues",
    "retention",
    "partitions",
    "consumers",
    "offsets",
    "snapshots",
    "replication",
    "compaction",
]
requests = {"robots": 0, "pages": 0, "other": 0}


def page(origin: int, number: int) -> bytes:
    children = [c for c in range(FANOUT * number + 1, FANOUT * number + FANOUT + 1) if c < PAGES]
    links = "".join(f'<li><a href="/p/{c}">Page {c}</a></li>' for c in children)
    topic = TOPICS[(origin * 7 + number) % len(TOPICS)]
    return (
        f"<!doctype html><html><head><title>Origin {origin} page {number}: {topic}</title></head><body><main>"
        f"<h1>Origin {origin} page {number}: {topic}</h1>"
        f"<p>This benchmark page number {number} on origin {origin} describes {topic} in a queueing system. "
        f"It exists so a crawler has distinct, extractable text to fetch: the identifier {origin}-{number} "
        f"appears nowhere else, which keeps every page unique and out of the duplicate-content filter.</p>"
        f"<h2>Related pages</h2><ul>{links}</ul></main></body></html>"
    ).encode()


def reply(status: str, body: bytes, kind: str) -> bytes:
    head = f"HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n"
    return head.encode() + body


async def handle(reader, writer, origin):
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            path = head.split(b" ", 2)[1].decode("ascii", "replace").split("?")[0]
            if path == "/robots.txt":
                requests["robots"] += 1
                out = reply("200 OK", ROBOTS, "text/plain")
            elif path == "/__stats":
                out = reply("200 OK", json.dumps(requests).encode(), "application/json")
            elif (m := re.fullmatch(r"/p/(\d+)", path)) and int(m.group(1)) < PAGES:
                requests["pages"] += 1
                await asyncio.sleep(LATENCY)
                out = reply("200 OK", page(origin, int(m.group(1))), "text/html; charset=utf-8")
            else:
                requests["other"] += 1
                out = reply("404 Not Found", b"not found\n", "text/plain")
            writer.write(out)
            await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
        pass
    finally:
        writer.close()


async def main():
    servers = [
        await asyncio.start_server(
            lambda r, w, o=origin: handle(r, w, o), "0.0.0.0", BASE_PORT + origin, backlog=512
        )
        for origin in range(ORIGINS)
    ]
    print(f"serving {ORIGINS} origins on ports {BASE_PORT}-{BASE_PORT + ORIGINS - 1}", flush=True)
    await asyncio.gather(*(server.serve_forever() for server in servers))


asyncio.run(main())
