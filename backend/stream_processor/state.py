from typing import Optional, Tuple


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


def update_temperature_state(
    state: Optional[TruckTemperatureState],
    event: dict,
) -> Tuple[TruckTemperatureState, dict]:
    """
    Update temperature state for one truck.

    Bytewax passes the previous state for the truck.
    """

    if state is None:
        state = TruckTemperatureState()

    temperature = float(event["temperature"])
    average = state.update(temperature)

    result = {
        "truck_id": event["truck_id"],
        "timestamp": event["timestamp"],
        "temperature": temperature,
        "average_temperature": round(average, 2),
        "count": state.count,
    }

    return state, result