import cv2
import numpy as np
import time
import json
from datetime import datetime, timezone
from collections import deque
import paho.mqtt.client as mqtt
from pathlib import Path

CAMERAS_CONFIG_FILE = Path(__file__).with_name("cameras.json")

WINDOW_WIDTH = 640
WINDOW_HEIGHT = 360

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "ods/i6/codigos/decodificados"
MQTT_CLIENT_ID = "i6-qrcode-reader"

QR_HISTORY_SECONDS = 1.5
QR_LOST_SECONDS = 3.0

ACQUIRE_CONFIRM_FRAMES = 4
SWITCH_CONFIRM_FRAMES = 10
SWITCH_AREA_RATIO = 1.5

def load_cameras():
    try:
        with open(
            CAMERAS_CONFIG_FILE,
            "r",
            encoding="utf-8"
        ) as file:
            cameras = json.load(file)

    except FileNotFoundError:
        print(
            f"Arquivo de câmeras não encontrado: "
            f"{CAMERAS_CONFIG_FILE}"
        )
        return []

    except json.JSONDecodeError as e:
        print(
            f"Erro no arquivo de configuração "
            f"das câmeras: {e}"
        )
        return []

    if not isinstance(cameras, list):
        print(
            "O arquivo cameras.json deve conter "
            "uma lista de câmeras."
        )
        return []

    valid_cameras = []
    camera_ids = set()

    for camera in cameras:
        if not isinstance(camera, dict):
            print(
                "Configuração de câmera inválida."
            )
            continue

        if (
            "source" not in camera
            or "camera_id" not in camera
        ):
            print(
                "Cada câmera deve possuir "
                "'source' e 'camera_id'."
            )
            continue

        camera_id = camera["camera_id"]

        if not isinstance(camera_id, str):
            print(
                "camera_id deve ser uma string."
            )
            continue

        if camera_id in camera_ids:
            print(
                f"camera_id duplicado: {camera_id}"
            )
            continue

        camera_ids.add(camera_id)

        valid_cameras.append(
            {
                "source": camera["source"],
                "camera_id": camera_id
            }
        )

    return valid_cameras

def current_timestamp():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

def connect_pubsub():
    print(
        f"Conectando ao Pub/Sub MQTT em "
        f"{MQTT_BROKER}:{MQTT_PORT}..."
    )

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID
    )

    try:
        client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            keepalive=60
        )

        client.loop_start()

        print("Conectado ao Pub/Sub!")

        return client

    except Exception as e:
        print(
            f"Erro ao conectar ao Pub/Sub: {e}"
        )

        return None

def publish_qrcode_event(
    mqtt_client,
    decoded_data,
    captured_at,
    camera_id
):
    payload = {
        "captured_at": captured_at,
        "camera_id": camera_id,
        "code_type": "QR_CODE",
        "decoded_data": decoded_data
    }

    message = {
        "message_type": "event",
        "schema": "ods.codigos.i6",
        "schema_version": "1.0",
        "producer": "I6",
        "published_at": current_timestamp(),
        "payload": payload
    }

    json_message = json.dumps(
        message,
        ensure_ascii=False
    )

    result = mqtt_client.publish(
        MQTT_TOPIC,
        json_message,
        qos=1
    )

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(
            f"\nEvento publicado por {camera_id}:"
        )

        print(
            json.dumps(
                message,
                indent=4,
                ensure_ascii=False
            )
        )

    else:
        print(
            f"Erro ao publicar mensagem da "
            f"{camera_id}. Código MQTT: {result.rc}"
        )

def detect_and_decode(
    frame,
    detector
):
    b_channel = frame[:, :, 0]

    _, binarized = cv2.threshold(
        b_channel,
        0,
        255,
        cv2.THRESH_BINARY | cv2.THRESH_OTSU
    )

    texts, points = detector.detectAndDecode(
        frame
    )

    candidates = []

    if texts and points is not None:
        for text, pts in zip(
            texts,
            points
        ):
            if not text:
                continue

            pts = np.array(
                pts,
                dtype=np.float32
            )

            area = cv2.contourArea(
                pts
            )

            pts_int = pts.astype(
                np.int32
            )

            x, y, w, h = cv2.boundingRect(
                pts_int
            )

            candidates.append(
                {
                    "text": text,
                    "rect": (x, y, w, h),
                    "area": area
                }
            )

    return candidates, binarized

def connect_camera(source):
    print(
        f"Conectando à câmera: {source}"
    )

    cap = cv2.VideoCapture(
        source
    )

    cap.set(
        cv2.CAP_PROP_BUFFERSIZE,
        1
    )

    if not cap.isOpened():
        print(
            f"Não foi possível abrir "
            f"a câmera {source}."
        )

        return None

    print(
        f"Câmera {source} conectada!"
    )

    return cap

def create_camera_state(
    source,
    camera_id
):
    cap = connect_camera(
        source
    )

    if cap is None:
        return None

    detector = (
        cv2.wechat_qrcode_WeChatQRCode()
    )

    return {
        "source": source,
        "camera_id": camera_id,
        "cap": cap,
        "detector": detector,
        "history": {},
        "last_seen": {},
        "active_qr": None,
        "active_reference_area": 0,
        "active_last_seen": 0,
        "acquisition_candidate": None,
        "acquisition_frames": 0,
        "switch_candidate": None,
        "switch_frames": 0
    }

def get_stable_area(
    state,
    text
):
    history = state["history"]

    if text not in history:
        return 0

    areas = [
        value[1]
        for value in history[text]
    ]

    if not areas:
        return 0

    return float(
        np.median(areas)
    )

def process_camera(
    state,
    mqtt_client
):
    cap = state["cap"]

    ret, frame = cap.read()

    if not ret:
        print(
            f"Frame não recebido da "
            f"{state['camera_id']}."
        )

        cap.release()

        time.sleep(0.2)

        new_cap = connect_camera(
            state["source"]
        )

        if new_cap is not None:
            state["cap"] = new_cap

        return None, None

    frame = cv2.flip(
        frame,
        1
    )

    frame = cv2.convertScaleAbs(
        frame,
        alpha=0.9,
        beta=-30
    )

    candidates, binarized = (
        detect_and_decode(
            frame,
            state["detector"]
        )
    )

    now = time.monotonic()

    history = state["history"]

    for candidate in candidates:
        text = candidate["text"]

        if text not in history:
            history[text] = deque()

        history[text].append(
            (
                now,
                candidate["area"]
            )
        )

        state["last_seen"][text] = now

    for text in list(
        history.keys()
    ):
        while (
            history[text]
            and now - history[text][0][0]
            > QR_HISTORY_SECONDS
        ):
            history[text].popleft()

        if not history[text]:
            del history[text]

    candidates_by_text = {
        candidate["text"]: candidate
        for candidate in candidates
    }

    selected = None

    if state["active_qr"] is None:

        if candidates:
            best = max(
                candidates,
                key=lambda candidate:
                get_stable_area(
                    state,
                    candidate["text"]
                )
            )

            if (
                state[
                    "acquisition_candidate"
                ]
                == best["text"]
            ):
                state[
                    "acquisition_frames"
                ] += 1

            else:
                state[
                    "acquisition_candidate"
                ] = best["text"]

                state[
                    "acquisition_frames"
                ] = 1

            if (
                state[
                    "acquisition_frames"
                ]
                >= ACQUIRE_CONFIRM_FRAMES
            ):
                state["active_qr"] = (
                    best["text"]
                )

                state[
                    "active_reference_area"
                ] = get_stable_area(
                    state,
                    state["active_qr"]
                )

                state[
                    "active_last_seen"
                ] = now

                selected = best

                publish_qrcode_event(
                    mqtt_client,
                    state["active_qr"],
                    current_timestamp(),
                    state["camera_id"]
                )

                state[
                    "acquisition_candidate"
                ] = None

                state[
                    "acquisition_frames"
                ] = 0

        else:
            state[
                "acquisition_candidate"
            ] = None

            state[
                "acquisition_frames"
            ] = 0

    else:

        active_candidate = (
            candidates_by_text.get(
                state["active_qr"]
            )
        )

        if active_candidate is not None:

            state[
                "active_last_seen"
            ] = now

            current_active_area = (
                get_stable_area(
                    state,
                    state["active_qr"]
                )
            )

            state[
                "active_reference_area"
            ] = max(
                current_active_area,
                state[
                    "active_reference_area"
                ] * 0.98
            )

            selected = active_candidate

        active_missing_time = (
            now
            - state[
                "active_last_seen"
            ]
        )

        challengers = [
            candidate
            for candidate in candidates
            if candidate["text"]
            != state["active_qr"]
        ]

        if (
            active_missing_time
            < QR_LOST_SECONDS
        ):

            if challengers:

                best_challenger = max(
                    challengers,
                    key=lambda candidate:
                    get_stable_area(
                        state,
                        candidate["text"]
                    )
                )

                challenger_area = (
                    get_stable_area(
                        state,
                        best_challenger[
                            "text"
                        ]
                    )
                )

                required_area = (
                    state[
                        "active_reference_area"
                    ]
                    * SWITCH_AREA_RATIO
                )

                if (
                    challenger_area
                    >= required_area
                ):

                    if (
                        state[
                            "switch_candidate"
                        ]
                        ==
                        best_challenger[
                            "text"
                        ]
                    ):

                        state[
                            "switch_frames"
                        ] += 1

                    else:

                        state[
                            "switch_candidate"
                        ] = best_challenger[
                            "text"
                        ]

                        state[
                            "switch_frames"
                        ] = 1

                    if (
                        state[
                            "switch_frames"
                        ]
                        >= SWITCH_CONFIRM_FRAMES
                    ):

                        state[
                            "active_qr"
                        ] = best_challenger[
                            "text"
                        ]

                        state[
                            "active_reference_area"
                        ] = challenger_area

                        state[
                            "active_last_seen"
                        ] = now

                        selected = (
                            best_challenger
                        )

                        publish_qrcode_event(
                            mqtt_client,
                            state[
                                "active_qr"
                            ],
                            current_timestamp(),
                            state[
                                "camera_id"
                            ]
                        )

                        state[
                            "switch_candidate"
                        ] = None

                        state[
                            "switch_frames"
                        ] = 0

                else:
                    state[
                        "switch_candidate"
                    ] = None

                    state[
                        "switch_frames"
                    ] = 0

            else:
                state[
                    "switch_candidate"
                ] = None

                state[
                    "switch_frames"
                ] = 0

        else:
            state["active_qr"] = None

            state[
                "active_reference_area"
            ] = 0

            state[
                "switch_candidate"
            ] = None

            state[
                "switch_frames"
            ] = 0

            state[
                "acquisition_candidate"
            ] = None

            state[
                "acquisition_frames"
            ] = 0

    if selected is not None:

        x, y, w, h = (
            selected["rect"]
        )

        cv2.rectangle(
            frame,
            (x, y),
            (x + w, y + h),
            (0, 255, 0),
            3
        )

        cv2.putText(
            frame,
            selected["text"],
            (
                x,
                max(y - 10, 20)
            ),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            (0, 255, 0),
            2
        )

    return frame, binarized

def main():
    cameras = load_cameras()

    if not cameras:
        print(
            "Nenhuma câmera configurada."
        )
        return

    print(
        f"{len(cameras)} câmera(s) "
        f"carregada(s)."
    )

    for camera in cameras:
        print(
            f"{camera['camera_id']} -> "
            f"{camera['source']}"
        )

    mqtt_client = connect_pubsub()

    if mqtt_client is None:
        return

    camera_states = []

    for camera_config in cameras:
        state = create_camera_state(
            camera_config["source"],
            camera_config["camera_id"]
        )

        if state is not None:
            camera_states.append(
                state
            )

            normal_window = (
                f"{state['camera_id']} - Decodificador"
            )

            binary_window = (
                f"{state['camera_id']} - Binarizada"
            )

            cv2.namedWindow(
                normal_window,
                cv2.WINDOW_NORMAL
            )

            cv2.namedWindow(
                binary_window,
                cv2.WINDOW_NORMAL
            )

            cv2.resizeWindow(
                normal_window,
                WINDOW_WIDTH,
                WINDOW_HEIGHT
            )

            cv2.resizeWindow(
                binary_window,
                WINDOW_WIDTH,
                WINDOW_HEIGHT
            )

    if not camera_states:
        print(
            "Nenhuma câmera disponível."
        )

        mqtt_client.loop_stop()
        mqtt_client.disconnect()

        return

    while True:

        for state in camera_states:

            frame, binarized = (
                process_camera(
                    state,
                    mqtt_client
                )
            )

            if frame is None:
                continue

            cv2.imshow(
                f"{state['camera_id']} - Decodificador",
                frame
            )

            cv2.imshow(
                f"{state['camera_id']} - Binarizada",
                binarized
            )

        if (
            cv2.waitKey(1) & 0xFF
            == ord("q")
        ):
            break

    for state in camera_states:
        state["cap"].release()

    cv2.destroyAllWindows()

    mqtt_client.loop_stop()
    mqtt_client.disconnect()

if __name__ == "__main__":
    main()