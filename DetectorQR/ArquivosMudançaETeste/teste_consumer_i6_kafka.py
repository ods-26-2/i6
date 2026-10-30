import os
import json
import uuid

from confluent_kafka import Consumer


BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "34.66.235.91:9093"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ods.i6.code_read"
)

consumer = Consumer(
    {
        "bootstrap.servers": BROKER,
        "group.id":
            f"teste-i6-{uuid.uuid4()}",
        "auto.offset.reset": "latest"
    }
)

consumer.subscribe(
    [TOPIC]
)

print(
    f"Aguardando eventos em {TOPIC}..."
)

try:
    while True:
        msg = consumer.poll(
            1.0
        )

        if msg is None:
            continue

        if msg.error():
            print(
                "Erro:",
                msg.error()
            )

            continue

        evento = json.loads(
            msg.value().decode(
                "utf-8"
            )
        )

        print(
            "\nEvento recebido:"
        )

        print(
            json.dumps(
                evento,
                indent=2,
                ensure_ascii=False
            )
        )

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
