def get_when_to_notify(
    data_length: int, notification_percentage_interval: int
) -> list[int]:
    notify_at_indexes: list[int] = []
    for i in range(1, int(100 / notification_percentage_interval) - 1):
        notify_at_index = int(
            data_length * (notification_percentage_interval / 100) * i
        )
        notify_at_indexes.append(notify_at_index + 1)
    return notify_at_indexes
