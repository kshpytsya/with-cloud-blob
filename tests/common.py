import time
import typing as tp

DYNAMODB_ENDPOINT = "http://localhost:8770"
ENDPOINT = "http://localhost:8771"


def s3_modify_options_dict(
    *,
    delay_put: float,
) -> tp.Dict[str, str]:
    return {
        "endpoint": ENDPOINT,
        "dynamodb_endpoint": DYNAMODB_ENDPOINT,
        "region": "us-east-1",
        "_delay_put": str(delay_put),
        "max_lag": str(max(2, int(delay_put * 30))),
        "lag_retry_period": "0.1",
        "dynamodb_table": f"with-cloud-blob-test-{time.time_ns()}",
    }
