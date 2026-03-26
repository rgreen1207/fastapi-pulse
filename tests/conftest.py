import pytest
from fastapi_pulse.backends.memory import MemoryBroker


@pytest.fixture
def memory_broker():
    return MemoryBroker()
