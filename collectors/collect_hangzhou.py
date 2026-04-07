#!/usr/bin/env python3
"""Backward-compatible wrapper for the Zhaopin-only collector."""

from __future__ import annotations

from collectors.collect_zhilian import main


if __name__ == '__main__':
    raise SystemExit(main())
