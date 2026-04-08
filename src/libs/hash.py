import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def calculate_sha256_hash(obj: Any) -> str:
    norm_obj = normalized_obj(obj)
    norm_json = json.dumps(norm_obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(norm_json.encode("utf-8")).hexdigest()


def normalized_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): normalized_obj(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        items = [normalized_obj(v) for v in obj]
        try:
            items.sort(
                key=lambda x: json.dumps(
                    x, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                )
            )
        except Exception:
            # fallback to original order if something goes wrong
            pass
        return items

    if isinstance(obj, set):
        items = [normalized_obj(v) for v in obj]
        items.sort(
            key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        )
        return items

    if isinstance(obj, bytes):
        return obj.hex()

    if isinstance(obj, Decimal):
        return str(obj)

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, date):
        return obj.isoformat()

    if obj is None or isinstance(obj, (str, int, float, bool)):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return str(obj)

        return obj

    if hasattr(obj, "__dict__"):
        return normalized_obj(obj.__dict__)

    return str(obj)
