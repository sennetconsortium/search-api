from datetime import datetime

from bson.codec_options import TypeDecoder


class DatetimeDecoder(TypeDecoder):
    bson_type = datetime  # type: ignore

    def transform_bson(self, value: datetime) -> int:
        return int(value.timestamp() * 1000)
