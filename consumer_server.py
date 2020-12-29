import json
from kafka import KafkaConsumer
from tornado import gen
import asyncio

def consumer():
    x = KafkaConsumer(
        "sf_crime",
        group_id="sf_crime_0",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m : json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False)
    return x

async def consume():
    c = consumer()
    for msg in c:
        print(f"{msg.offset}", msg.value)
        await gen.sleep(1)

if __name__ == "__main__":
    asyncio.run(consume())
