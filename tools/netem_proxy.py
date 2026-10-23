#!/usr/bin/env python3
"""Tiny TCP proxy that emulates a slower network between a client and the server -- no root, no kernel modules.

    python3 tools/netem_proxy.py --listen-port 6000 --target 127.0.0.1:5000 --delay-ms 2 --rate-mbit 100

Clients connect to the proxy port instead of the server port. Every byte is delayed by --delay-ms in BOTH
directions (so RTT = 2 x delay) and each direction is capped at --rate-mbit. If the server resets a connection
(the fault injection does that) the proxy resets the client side too, so clients see the same failure.

It is a rehearsal tool: real tc/netem or the real lab network is more faithful, and Python limits the proxy to
a few hundred MB/s in total, so use a rate cap well below that.
"""
import argparse, asyncio, socket, struct, time

async def pump(reader, writer, other_writer, delay, rate):
    """Copy reader -> writer with a fixed delay and a bandwidth cap; propagate EOF / reset."""
    q = asyncio.Queue(maxsize=16)       # small: the emulated link must push back on the sender, not absorb megabytes

    async def rx():
        try:
            while True:
                data = await reader.read(65536)
                await q.put((time.monotonic() + delay, data or None, False))
                if not data:
                    return
        except (ConnectionError, OSError):
            await q.put((time.monotonic() + delay, None, True))

    async def tx():
        free_at = 0.0
        while True:
            due, data, reset = await q.get()
            now = time.monotonic()
            if data is None:
                if due > now: await asyncio.sleep(due - now)
                if reset:
                    other_writer.transport.abort()          # RST towards the other side
                    writer.transport.abort()
                else:
                    try:
                        if writer.can_write_eof(): writer.write_eof()
                    except OSError:
                        pass
                return
            if rate:
                free_at = max(free_at, now) + len(data) / rate
                due = max(due, free_at)
            if due > now: await asyncio.sleep(due - now)
            try:
                writer.write(data); await writer.drain()
            except (ConnectionError, OSError):
                return

    await asyncio.gather(rx(), tx())

def make_socket(buffer_kb):
    """TCP socket with small kernel buffers so that the proxy does not hide the bottleneck by buffering MBs."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if buffer_kb:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_kb * 1024)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buffer_kb * 1024)
    return s

async def handle(creader, cwriter, host, port, delay, rate, buffer_kb):
    try:
        us = make_socket(buffer_kb); us.setblocking(False)
        await asyncio.get_running_loop().sock_connect(us, (host, port))
        sreader, swriter = await asyncio.open_connection(sock=us)
    except OSError:
        cwriter.transport.abort(); return
    for w in (cwriter, swriter):
        s = w.get_extra_info("socket")
        if s: s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    await asyncio.gather(pump(creader, swriter, cwriter, delay, rate), pump(sreader, cwriter, swriter, delay, rate))
    for w in (cwriter, swriter):
        try: w.close()
        except OSError: pass

async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen-port", type=int, required=True)
    ap.add_argument("--target", required=True, help="host:port of the real server")
    ap.add_argument("--delay-ms", type=float, default=0.0, help="one-way delay (RTT = 2x)")
    ap.add_argument("--rate-mbit", type=float, default=0.0, help="per-direction, per-connection cap; 0 = unlimited")
    ap.add_argument("--buffer-kb", type=int, default=256, help="kernel socket buffer size used by the proxy (0 = OS default)")
    a = ap.parse_args()
    host, port = a.target.rsplit(":", 1)
    rate = a.rate_mbit * 1e6 / 8 if a.rate_mbit else 0
    ls = make_socket(a.buffer_kb)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("0.0.0.0", a.listen_port)); ls.listen(512); ls.setblocking(False)
    srv = await asyncio.start_server(lambda r, w: handle(r, w, host, int(port), a.delay_ms / 1000.0, rate, a.buffer_kb), sock=ls)
    print(f"[netem_proxy] :{a.listen_port} -> {a.target}  one-way delay {a.delay_ms} ms, cap "
          f"{a.rate_mbit or 'none'} Mbit/s per connection", flush=True)
    async with srv: await srv.serve_forever()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
