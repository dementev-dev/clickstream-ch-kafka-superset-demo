#!/usr/bin/env python3
"""
Скрипт для проверки сообщений в Kafka от генератора.
"""

import json
import sys
from kafka import KafkaConsumer


def verify_messages(bootstrap_servers="kafka:29092", timeout_ms=10000):
    """Проверяет сообщения в Kafka топиках."""
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    
    topics = ["browser_events", "location_events", "device_events", "geo_events"]
    
    for topic in topics:
        print(f"\n=== Topic: {topic} ===")
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="latest",
                consumer_timeout_ms=timeout_ms,
                enable_auto_commit=False,
                group_id="verify-group",
            )
            
            # Получаем информацию о партициях
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                print(f"  Partitions: {partitions}")
                
                # Смотрим end offsets
                end_offsets = consumer.end_offsets(
                    [consumer.cluster.topic_partitions(topic)]
                )
                print(f"  End offsets: {end_offsets}")
            
            # Читаем последние сообщения
            messages = []
            for msg in consumer:
                messages.append(msg)
                if len(messages) >= 5:
                    break
            
            if messages:
                print(f"  Last {len(messages)} messages:")
                for i, msg in enumerate(messages[:3]):
                    value = json.loads(msg.value.decode("utf-8"))
                    print(f"    [{i}] offset={msg.offset}, key={msg.key}, value={json.dumps(value)[:100]}...")
            else:
                print("  No new messages")
            
            consumer.close()
            
        except Exception as e:
            print(f"  Error: {e}")


if __name__ == "__main__":
    verify_messages()
