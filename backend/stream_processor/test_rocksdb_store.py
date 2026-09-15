from .rocksdb_store import RocksDBStateStore


def main():
    store = RocksDBStateStore()

    store.put(
        "TRUCK-00001",
        {
            "total": 125.5,
            "count": 5,
            "average": 25.1,
        },
    )

    state = store.get("TRUCK-00001")

    print("Stored state:")
    print(state)

    store.close()


if __name__ == "__main__":
    main()