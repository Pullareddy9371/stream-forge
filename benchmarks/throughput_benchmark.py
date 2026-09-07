import time
from concurrent.futures import ThreadPoolExecutor


PARTITIONS = 3
EVENTS_PER_PARTITION = 100_000


def process_event(event):
    """Simulate the core StreamForge processing work."""

    temperature = event.get("temperature")

    if temperature is None or temperature <= 0:
        return None

    result = dict(event)

    result["temperature_status"] = (
        "normal" if temperature <= 40 else "high"
    )

    return result


def generate_events(count, partition_id):
    """Generate telemetry events for one local partition."""

    return [
        {
            "truck_id": f"TRUCK-{i:05d}",
            "temperature": 25.5,
            "speed_kmph": 60.0,
            "partition": partition_id,
            "timestamp": "2026-09-06T10:00:00+00:00",
        }
        for i in range(count)
    ]


def process_partition(partition_id):
    """Generate and process events for one partition."""

    events = generate_events(
        EVENTS_PER_PARTITION,
        partition_id,
    )

    processed = 0

    for event in events:
        result = process_event(event)

        if result is not None:
            processed += 1

    return processed


def main():
    """Run the StreamForge throughput benchmark."""

    total_events = PARTITIONS * EVENTS_PER_PARTITION

    print("=" * 40)
    print("StreamForge Throughput Benchmark")
    print("=" * 40)
    print(f"Total events : {total_events:,}")
    print(f"Partitions   : {PARTITIONS}")
    print()

    start_time = time.perf_counter()

    with ThreadPoolExecutor(
        max_workers=PARTITIONS
    ) as executor:
        results = list(
            executor.map(
                process_partition,
                range(PARTITIONS),
            )
        )

    elapsed = time.perf_counter() - start_time

    processed_events = sum(results)

    throughput = (
        processed_events / elapsed
        if elapsed > 0
        else 0
    )

    print(f"Processed events : {processed_events:,}")
    print(f"Elapsed time     : {elapsed:.2f} sec")
    print(f"Throughput       : {throughput:,.2f} events/sec")
    print(f"Target           : 100,000 events/sec")
    print()

    if throughput >= 100_000:
        print("RESULT: PASS - 100K events/sec target achieved.")
    else:
        print("RESULT: BELOW TARGET - optimization required.")

    print("=" * 40)


if __name__ == "__main__":
    main()