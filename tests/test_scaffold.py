from fraud_engine.api import app
from fraud_engine.config import Settings


def test_fastapi_shell_registers_core_routes():
    paths = {route.path for route in app.routes}
    assert {"/system/health", "/transactions", "/reviews", "/metrics"} <= paths


def test_settings_defaults():
    settings = Settings(_env_file=None)
    assert settings.kafka_raw_topic == "transactions.raw"
