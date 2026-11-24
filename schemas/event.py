"""Event contract — shared by the pure-Python logic and the Spark job."""
from __future__ import annotations

# The canonical event shape. Producers must emit these fields.
#   event_id : str   (unique — used for dedupe)
#   ts       : str   (ISO-8601 UTC timestamp)
#   type     : str   (e.g. "purchase", "click", "signup")
#   user_id  : str
#   region   : str
#   amount   : float (>= 0; 0 for non-monetary events)
REQUIRED_FIELDS = ("event_id", "ts", "type", "user_id", "region", "amount")
EVENT_TYPES = {"purchase", "click", "signup", "refund", "view"}


def spark_schema():
    """Return the equivalent Spark StructType (imported lazily)."""
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("ts", StringType(), False),
            StructField("type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )
