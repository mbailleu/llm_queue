"""`python -m anthropic_proxy` / console-script entrypoint."""
from __future__ import annotations

import asyncio


def main() -> None:
    from .server import serve
    asyncio.run(serve())


if __name__ == "__main__":
    main()
