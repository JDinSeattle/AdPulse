"""Integrity-protected keyset cursors; release pinning is not an authorization mechanism."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time

from .common import canonical, digest

MAX_PAGE_SIZE = 500
CURSOR_TTL_SECONDS = 1800
MAX_CURSOR_LENGTH = 4096


def valid_release(value):
    return isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", value) is not None


class InvalidCursor(ValueError):
    pass


class CursorPositionTooLarge(ValueError):
    pass


class CursorCodec:
    def __init__(self, secret=None, clock=time.time):
        self.secret = secret or os.getenv("ADPULSE_CURSOR_SECRET", "").encode() or secrets.token_bytes(32)
        if len(self.secret) < 32:
            raise ValueError("ADPULSE_CURSOR_SECRET must contain at least 32 bytes")
        self.clock = clock

    def encode(self, *, release, kind, after, filters, expires_at=None):
        if not isinstance(after, str) or not 1 <= len(after) <= 1500:
            raise CursorPositionTooLarge("Result key exceeds the bounded cursor size")
        payload = canonical(dict(v=1, release=release, kind=kind, after=after, filters=digest(filters),
                                 expires_at=expires_at or int(self.clock()) + CURSOR_TTL_SECONDS)).encode()
        signature = hmac.digest(self.secret, payload, hashlib.sha256)
        token = base64.urlsafe_b64encode(payload + signature).decode().rstrip("=")
        if len(token) > MAX_CURSOR_LENGTH:
            raise CursorPositionTooLarge("Result key exceeds the bounded cursor size")
        return token

    def decode(self, token, *, kind, filters, release=None):
        try:
            if not isinstance(token, str) or not 1 <= len(token) <= MAX_CURSOR_LENGTH:
                raise ValueError("size")
            raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
            payload, signature = raw[:-32], raw[-32:]
            if not hmac.compare_digest(signature, hmac.digest(self.secret, payload, hashlib.sha256)):
                raise ValueError("signature")
            data = json.loads(payload)
            if (data["v"] != 1 or data["kind"] != kind or data["filters"] != digest(filters)
                    or not valid_release(data["release"]) or (release is not None and release != data["release"])
                    or type(data["expires_at"]) is not int or data["expires_at"] <= self.clock()
                    or not isinstance(data["after"], str) or not 1 <= len(data["after"]) <= 1500
                    or not data["after"].startswith("m:" if kind == "metric" else "a:")):
                raise ValueError("scope or expiry")
            return data
        except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
            raise InvalidCursor("Invalid, expired, or differently scoped cursor; restart pagination") from exc
