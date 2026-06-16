import os
import json
from urllib.parse import urlparse

import redis
import requests
from dotenv import load_dotenv


load_dotenv()


class UpstashRestClient:
    def __init__(self, base_url, token, timeout=3):
        self.base_url = (base_url or "").rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _decode_scalar(self, value):
        if isinstance(value, list):
            return [self._decode_scalar(item) for item in value]
        return value

    def _command(self, *parts):
        response = self.session.post(
            self.base_url,
            data=json.dumps([str(part) if part is not None else "" for part in parts]),
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])
        return self._decode_scalar(payload.get("result"))

    def ping(self):
        result = self._command("PING")
        return result == "PONG"

    def get(self, key):
        return self._command("GET", key)

    def set(self, key, value, ex=None):
        if ex is None:
            result = self._command("SET", key, value)
        else:
            result = self._command("SET", key, value, "EX", int(ex))
        return result == "OK"

    def incr(self, key):
        return self._command("INCR", key)

    def expire(self, key, seconds):
        return self._command("EXPIRE", key, int(seconds))

    def ttl(self, key):
        return self._command("TTL", key)

    def type(self, key):
        return self._command("TYPE", key)

    def delete(self, *keys):
        if not keys:
            return 0
        return self._command("DEL", *keys)

    def lpush(self, key, *values):
        return self._command("LPUSH", key, *values)

    def ltrim(self, key, start, stop):
        return self._command("LTRIM", key, int(start), int(stop)) == "OK"

    def llen(self, key):
        return self._command("LLEN", key)

    def zadd(self, key, mapping):
        args = ["ZADD", key]
        for member, score in mapping.items():
            args.extend([float(score), member])
        return self._command(*args)

    def zrevrange(self, key, start, stop, withscores=False):
        args = ["ZREVRANGE", key, int(start), int(stop)]
        if withscores:
            args.append("WITHSCORES")
        result = self._command(*args) or []
        if not withscores:
            return result
        parsed = []
        for idx in range(0, len(result), 2):
            member = result[idx]
            score = result[idx + 1] if idx + 1 < len(result) else 0
            parsed.append((member, float(score)))
        return parsed

    def scan_iter(self, match=None, count=100):
        cursor = "0"
        while True:
            args = ["SCAN", cursor]
            if match:
                args.extend(["MATCH", match])
            if count:
                args.extend(["COUNT", int(count)])
            result = self._command(*args) or ["0", []]
            cursor = str(result[0])
            for item in result[1] or []:
                yield item
            if cursor == "0":
                break

    def close(self):
        self.session.close()


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
        try:
            self.timeout_seconds = float(os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "2.5"))
        except Exception:
            self.timeout_seconds = 2.5
        self.client = None
        self.connect()

    def connect(self):
        if not self.url:
            return False

        try:
            parsed = urlparse(self.url if "://" in self.url else f"https://{self.url}")
            if parsed.scheme in ("http", "https"):
                self.client = UpstashRestClient(self.url, self.token, timeout=self.timeout_seconds)
            else:
                host = parsed.hostname or self.url
                port = parsed.port or 6379
                use_ssl = parsed.scheme == "rediss"
                self.client = redis.Redis(host=host, port=port, password=self.token, ssl=use_ssl, socket_timeout=self.timeout_seconds)
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

    def lpush_json(self, key, obj, max_len=None):
        if not self.client:
            return False
        try:
            self.client.lpush(key, json.dumps(obj))
            if isinstance(max_len, int) and max_len > 0:
                self.client.ltrim(key, 0, max_len - 1)
            return True
        except Exception:
            return False

    def zadd_scores(self, key, mapping, ex=None):
        if not self.client:
            return False
        try:
            if not mapping:
                return False
            self.client.zadd(key, mapping)
            if ex is not None:
                self.client.expire(key, int(ex))
            return True
        except Exception:
            return False

    def zrevrange_with_scores(self, key, start=0, stop=9):
        if not self.client:
            return []
        try:
            rows = self.client.zrevrange(key, int(start), int(stop), withscores=True)
            parsed = []
            for member, score in rows:
                member_text = member.decode("utf-8") if isinstance(member, (bytes, bytearray)) else str(member)
                parsed.append((member_text, float(score)))
            return parsed
        except Exception:
            return []

    def incr_counter(self, key):
        if not self.client:
            return None
        try:
            return int(self.client.incr(key))
        except Exception:
            return None

    def get_counter(self, key):
        value = self.get_text(key)
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except Exception:
            return 0

    def incr_property_views(self, propiedad_id):
        key = f"contador:vistas:{str(propiedad_id).strip()}"
        if key.endswith(":"):
            return None
        return self.incr_counter(key)

    def get_property_views(self, propiedad_id):
        key = f"contador:vistas:{str(propiedad_id).strip()}"
        if key.endswith(":"):
            return 0
        return self.get_counter(key)

    def cache_top_properties_by_city(self, ciudad, property_scores, ttl_seconds=3600):
        city_token = (ciudad or "").strip().casefold() or "global"
        key = f"top:propiedades:{city_token}"
        mapping = {str(prop_id): float(score) for prop_id, score in (property_scores or {}).items()}
        return self.zadd_scores(key, mapping, ex=ttl_seconds)

    def get_top_properties_by_city(self, ciudad, limit=10):
        city_token = (ciudad or "").strip().casefold() or "global"
        key = f"top:propiedades:{city_token}"
        return self.zrevrange_with_scores(key, 0, max(int(limit) - 1, 0))

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
