"""Inspect event logs produced by Quorum JSONL sinks.

Usage:

    python -m quorum.tail /path/to/log.jsonl          # watch mode
    python -m quorum.tail /path/to/log.jsonl --replay  # print all lines
    python -m quorum.tail /path/to/log.jsonl --filter finding.created
    python -m quorum.tail /path/to/log.jsonl --correlation run-1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="quorum.tail")
    parser.add_argument("path", type=Path, help="JSONL event log")
    parser.add_argument(
        "--replay",
        action="store_true",
        help="print every line and exit instead of watching for new lines",
    )
    parser.add_argument(
        "--filter",
        dest="filter_type",
        default=None,
        help="show only events of this type",
    )
    parser.add_argument(
        "--correlation",
        default=None,
        help="show only events with this correlation ID",
    )
    args = parser.parse_args(argv)

    if args.replay:
        _replay(args.path, args.filter_type, args.correlation)
    else:
        _watch(args.path, args.filter_type, args.correlation)


def _replay(path: Path, filter_type: str | None, correlation_id: str | None) -> None:
    if not path.is_file():
        print(f"not found: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path) as source:
        for line in source:
            _emit(line, filter_type, correlation_id)


def _watch(path: Path, filter_type: str | None, correlation_id: str | None) -> None:
    if not path.is_file():
        print(f"waiting for {path} ...", file=sys.stderr)
        while not path.is_file():
            time.sleep(0.25)

    with open(path) as source:
        source.seek(0, os.SEEK_END)
        try:
            while True:
                line = source.readline()
                if line:
                    _emit(line, filter_type, correlation_id)
                else:
                    time.sleep(0.1)
        except KeyboardInterrupt:
            pass


def _emit(line: str, filter_type: str | None, correlation_id: str | None) -> None:
    if filter_type is not None or correlation_id is not None:
        try:
            event: dict[str, Any] = json.loads(line)
        except json.JSONDecodeError:
            sys.stdout.write(line)
            return
        if filter_type is not None and event.get("type") != filter_type:
            return
        if correlation_id is not None and event.get("correlation_id") != correlation_id:
            return
    sys.stdout.write(line)


if __name__ == "__main__":
    main()
