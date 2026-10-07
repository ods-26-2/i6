import json
import paho.mqtt.client as mqtt

mensagem = {
    "message_type": "query",
    "schema": "ods.codigos.i6",
    "schema_version": "1.0",
    "producer": "teste",
    "published_at": "2026-10-06T00:00:00Z",
    "payload": {
        "request_id": "teste001",
        "camera_id": "camera01"
    }
}

cliente = mqtt.Client()

cliente.connect("localhost", 1883)

cliente.publish(
    "ods/codigos/i6/query",
    json.dumps(mensagem)
)

cliente.disconnect()

print("Query enviada!")
print(json.dumps(mensagem, indent=4))