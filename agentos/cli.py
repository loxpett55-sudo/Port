"""CLI платформы: `python -m agentos serve|health|chat`."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="agentos", description="AgentOS CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="запустить платформу с API и Dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--backend", choices=["memory", "sqlite"], default="sqlite")
    serve.add_argument("--data", default="./data", help="каталог durable-хранилищ")

    health = sub.add_parser("health", help="здоровье удалённой платформы")
    health.add_argument("--url", default="http://127.0.0.1:8080")

    chat = sub.add_parser("chat", help="сообщение агенту удалённой платформы")
    chat.add_argument("agent")
    chat.add_argument("message")
    chat.add_argument("--url", default="http://127.0.0.1:8080")

    args = parser.parse_args(argv)

    if args.command == "serve":
        return asyncio.run(_serve(args))
    if args.command == "health":
        from agentos.sdk import AgentOSClient

        print(json.dumps(AgentOSClient(args.url).health(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "chat":
        from agentos.sdk import AgentOSClient

        reply = AgentOSClient(args.url).send(args.agent, args.message)
        print(reply["text"])
        return 0
    return 1


async def _serve(args) -> int:
    from agentos.platform import AgentOSPlatform
    from agentos.runtimes.api import ApiRuntime

    platform = AgentOSPlatform(
        {"storage-runtime": {"backend": args.backend, "path": args.data}}
    )
    api = ApiRuntime(platform, host=args.host, port=args.port)
    platform._modules.append(api)
    await platform.start()
    print(f"AgentOS запущен: http://{args.host}:{api.server.port}/ (Ctrl+C — остановка)")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await platform.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
