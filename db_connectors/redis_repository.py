import os
import json
from urllib.parse import urlparse

import redis


class RedisRepository:
    """Simple Redis wrapper used as cache (supports Upstash-style URL+token).

    Usage:
        repo = RedisRepository()
        repo.set_text('key', 'value', ex=300)
        v = repo.get_text('key')
    """

    def __init__(self, url=None, token=None):
        self.url = url or os.getenv("UPSTASH_REDIS_REST_URL")
        self.token = token or os.getenv("UPSTASH_REDIS_REST_TOKEN")
        self.client = None
        self.connect()

    def connect(self):
        if not self.url:
            return False

        try:
            parsed = urlparse(self.url if "://" in self.url else f"https://{self.url}")
            host = parsed.hostname or self.url
            port = parsed.port or (6379 if parsed.scheme in ("redis", "rediss") else 6379)
            # Upstash recommends using TLS and password (token)
            self.client = redis.Redis(host=host, port=port, password=self.token, ssl=True, socket_timeout=3)
            # quick ping to validate
            self.client.ping()
            return True
        except Exception:
            self.client = None
            return False

    def get_text(self, key):
        if not self.client:
            return None
        try:
            val = self.client.get(key)
            return val.decode('utf-8') if isinstance(val, (bytes, bytearray)) else val
        except Exception:
            return None

    def set_text(self, key, value, ex=None):
        if not self.client:
            return False
        try:
            return self.client.set(key, value, ex=ex)
        except Exception:
            return False

    def get_json(self, key):
        txt = self.get_text(key)
        if not txt:
            return None
        try:
            return json.loads(txt)
        except Exception:
            return None

    def set_json(self, key, obj, ex=None):
        try:
            txt = json.dumps(obj)
            return self.set_text(key, txt, ex=ex)
        except Exception:
            return False

    def close(self):
        try:
            if self.client:
                # redis-py has a close method
                self.client.close()
        finally:
            self.client = None

    def ping(self):
        if not self.client:
            return False
        try:
            return self.client.ping()
        except Exception:
            return False
