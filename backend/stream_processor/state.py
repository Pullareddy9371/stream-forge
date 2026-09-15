from typing import Optional, Tuple

from .rocksdb_store import RocksDBStateStore


class TruckTemperatureState:
    """Maintain temperature aggregation state for one truck."""

    def __init__(self, total: float = 0.0, count: int = 0):
        self.total = total
        self.count = count

    @property
    def average(self) -> float:
        """Return the current average temperature."""
        if self.count == 0:
            return 0.0

        return self.total / self.count

    def update(self, temperature: float) -> float:
        """Add a temperature reading and return the new average."""
        self.total += temperature
        self.count += 1

        return self.average


# Persistent RocksDB store shared by the stream processor.
state_store = RocksDBStateStore()


def update_temperature_state(
    state: Optional[TruckTemperatureState],
    event: dict,
) -> Tuple[TruckTemperatureState, dict]:
    """
    Update per-truck temperature state and persist it in RocksDB.
    """

    truck_id = event["truck_id"]

    # If Bytewax has no in-memory state, try to restore it from RocksDB.
    if state is None:
        saved_state = state_store.get(truck_id)

        if saved_state is not None:
            state = TruckTemperatureState(
                total=float(saved_state["total"]),
                count=int(saved_state["count"]),
            )
        else:
            state = TruckTemperatureState()

    temperature = float(event["temperature"])

    average = state.update(temperature)

    # Persist the latest state in RocksDB.
    state_store.put(
        truck_id,
        {
            "total": state.total,
            "count": state.count,
            "average": state.average,
        },
    )

    result = {
        "truck_id": truck_id,
        "timestamp": event["timestamp"],
        "temperature": temperature,
        "average_temperature": round(average, 2),
        "count": state.count,
    }

    return state, result