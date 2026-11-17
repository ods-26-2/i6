
"""
Mock simples da saída do P4 via socket TCP.

É apenas uma ferramenta de teste para validar o lado I6.

Protocolo:
    4 bytes -> tamanho do JSON
    N bytes -> JSON ods.p4.frame
    8 bytes -> tamanho do frame
    M bytes -> frame BGR

Uso:
    python mock_p4.py http://IP:PORTA/video cam0
"""

import sys
import cv2
import json
import uuid
import socket
import struct
from datetime import datetime, timezone


HOST = "0.0.0.0"
PORT = 5000


def agora_iso():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def enviar_frame(conexao, evento, frame):
    metadata_bytes = json.dumps(
        evento,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    frame_bytes = frame.tobytes()

    conexao.sendall(
        struct.pack(
            "!I",
            len(metadata_bytes)
        )
    )

    conexao.sendall(
        metadata_bytes
    )

    conexao.sendall(
        struct.pack(
            "!Q",
            len(frame_bytes)
        )
    )

    conexao.sendall(
        frame_bytes
    )


if len(sys.argv) < 2:
    print(
        "Uso: python mock_p4.py "
        "<url-ou-video> [source_id]"
    )
    raise SystemExit(1)


SOURCE = sys.argv[1]

SOURCE_ID = (
    sys.argv[2]
    if len(sys.argv) >= 3
    else "cam0"
)

cap = cv2.VideoCapture(
    SOURCE
)

if not cap.isOpened():
    raise SystemExit(
        f"Não foi possível abrir {SOURCE}"
    )


servidor = socket.socket(
    socket.AF_INET,
    socket.SOCK_STREAM
)

servidor.setsockopt(
    socket.SOL_SOCKET,
    socket.SO_REUSEADDR,
    1
)

servidor.bind(
    (
        HOST,
        PORT
    )
)

servidor.listen(1)

session_id = str(
    uuid.uuid4()
)

frame_index = 0

print(
    f"[MOCK P4] aguardando I6 em "
    f"{HOST}:{PORT}..."
)

try:
    while True:
        conexao, endereco = servidor.accept()

        print(
            f"[MOCK P4] I6 conectado: "
            f"{endereco}"
        )

        try:
            while True:
                ok, frame = cap.read()

                if (
                    not ok
                    or frame is None
                ):
                    raise RuntimeError(
                        "Fonte de vídeo indisponível."
                    )

                altura, largura = (
                    frame.shape[:2]
                )

                fps = cap.get(
                    cv2.CAP_PROP_FPS
                )

                if (
                    not fps
                    or fps <= 0
                ):
                    fps = 30.0

                evento = {
                    "schema":
                        "ods.p4.frame",

                    "producer":
                        "P4",

                    "captured_at":
                        agora_iso(),

                    "source_id":
                        SOURCE_ID,

                    "session_id":
                        session_id,

                    "payload": {
                        "frame_index":
                            frame_index,

                        "image_width":
                            largura,

                        "image_height":
                            altura,

                        "pixel_format":
                            "BGR",

                        "fps":
                            float(fps),

                        "raw":
                            True
                    }
                }

                enviar_frame(
                    conexao,
                    evento,
                    frame
                )

                frame_index += 1

        except (
            BrokenPipeError,
            ConnectionResetError
        ):
            print(
                "[MOCK P4] I6 desconectou. "
                "Aguardando novamente..."
            )

        finally:
            conexao.close()

finally:
    cap.release()
    servidor.close()
