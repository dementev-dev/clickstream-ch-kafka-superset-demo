"""Переходный тиковый слой генератора."""

from clickstream_generator.generation import EventGenerator


def generate_tick_batch(generator: EventGenerator, event_budget: int) -> dict[str, list[dict]]:
    """Набирает тиковый батч из одного или нескольких полных визитов.

    Это временный механизм до задачи 03. В ней тиковый слой начнёт хранить
    активные визиты и выпускать только созревшие события.
    """
    tick_batch = {
        "browser_events": [],
        "location_events": [],
        "device_events": [],
        "geo_events": [],
    }

    remaining_events = event_budget
    while remaining_events > 0:
        visit_batch = generator.generate_batch(remaining_events)
        generated_events = len(visit_batch["browser_events"])
        if generated_events == 0:
            break

        for topic, events in visit_batch.items():
            tick_batch[topic].extend(events)
        remaining_events -= generated_events

    return tick_batch
