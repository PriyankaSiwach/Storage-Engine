#!/usr/bin/env python3
"""Tiny REPL for the minilsm Stage 1 DB.

Commands: put <k> <v> | get <k> | delete <k> | quit
"""

from __future__ import annotations

import sys
from pathlib import Path

from minilsm import DB


def main() -> None:
    db_dir = sys.argv[1] if len(sys.argv) > 1 else "./data"
    Path(db_dir).mkdir(parents=True, exist_ok=True)
    db = DB.open(db_dir)
    print(f"minilsm open at {db_dir}")
    print("commands: put <k> <v> | get <k> | delete <k> | quit")

    try:
        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                print()
                break
            if not line:
                continue

            parts = line.split(maxsplit=2)
            cmd = parts[0].lower()

            if cmd == "quit":
                break
            elif cmd == "put" and len(parts) == 3:
                db.put(parts[1], parts[2])
                print("ok")
            elif cmd == "get" and len(parts) == 2:
                value = db.get(parts[1])
                print("(none)" if value is None else value)
            elif cmd == "delete" and len(parts) == 2:
                db.delete(parts[1])
                print("ok")
            else:
                print("usage: put <k> <v> | get <k> | delete <k> | quit")
    finally:
        db.close()


if __name__ == "__main__":
    main()
