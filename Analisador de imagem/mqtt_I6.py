import json
import cv2
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from Image_Analyzer import analisar_frame

# CONFIGURAÇÕES
BROKER = "localhost"
PORTA = 1883

TOPICO_QUERY = "ods/codigos/i6/query"
TOPICO_REPLY = "ods/codigos/i6/reply"

# True  = usa camera
# False = usa imagem fixa
USAR_CAMERA = False

# CONFIGURAÇÃO DA IMAGEM DE TESTE
CAMINHO_IMAGEM = "imagemdeteste.jpg"

# CONFIGURAÇÃO DA CAMERA
NUMERO_CAMERA = 0
# Guarda o último frame capturado
ultimo_frame = None
# Controle para acesso seguro ao frame
frame_lock = threading.Lock()
# Indica se a camera está funcionando
camera_ativa = False

def timestamp_atual():
    return datetime.now(
        timezone.utc
    ).isoformat(
        timespec="milliseconds"
    ).replace(
        "+00:00",
        "Z"
    )

def capturar_camera():
    global ultimo_frame
    global camera_ativa

    print("\nAbrindo camera...")

    camera = cv2.VideoCapture(NUMERO_CAMERA)

    if not camera.isOpened():
        print("ERRO: não foi possível abrir a camera.")
        camera_ativa = False
        return

    camera_ativa = True

    print("Camera aberta com sucesso!")
    print("Pressione Q na janela da camera para encerrar.")

    while camera_ativa:
        sucesso, frame = camera.read()
        if not sucesso:
            print("ERRO: não foi possível capturar frame.")
            break

        with frame_lock:
            ultimo_frame = frame.copy()

        cv2.imshow(
            "Camera - I6",
            frame
        )

        # Pressionar Q encerra a camera
        if cv2.waitKey(1) & 0xFF == ord("q"):
            camera_ativa = False
            break

    camera.release()
    cv2.destroyAllWindows()

    print("Camera encerrada.")

def obter_imagem():

    # MODO IMAGEM
    if not USAR_CAMERA:
        print(
            f"\nUsando imagem de teste: "
            f"{CAMINHO_IMAGEM}"
        )

        imagem = cv2.imread(
            CAMINHO_IMAGEM
        )

        if imagem is None:
            raise ValueError(
                f"Não foi possível carregar a imagem:\n"
                f"{CAMINHO_IMAGEM}"
            )
        return imagem

    # MODO CAMERA
    with frame_lock:
        if ultimo_frame is None:
            raise ValueError(
                "Nenhum frame da camera está disponível."
            )
        return ultimo_frame.copy()

# MQTT - CONEXÃO
def ao_conectar(
    client,
    userdata,
    flags,
    reason_code,
    properties
):
    if reason_code.is_failure:
        print(
            f"Falha ao conectar ao MQTT: "
            f"{reason_code}"
        )
        return

    print("\nConectado ao Mosquitto!")

    client.subscribe(
        TOPICO_QUERY
    )
    print(
        f"Escutando: {TOPICO_QUERY}"
    )


# ============================================================
# MQTT - RECEBIMENTO DA QUERY
# ============================================================

def ao_receber_mensagem(
    client,
    userdata,
    msg
):

    print("\n======================================")
    print("Query recebida!")
    print("======================================")

    conteudo = msg.payload.decode(
        "utf-8"
    )

    print("\nConteúdo recebido:")
    print(conteudo)

    try:

        # ----------------------------------------------------
        # CONVERTE JSON
        # ----------------------------------------------------

        mensagem = json.loads(
            conteudo
        )

        print(
            "\nJSON convertido com sucesso!"
        )

        # ----------------------------------------------------
        # VALIDA MESSAGE TYPE
        # ----------------------------------------------------

        if mensagem.get(
            "message_type"
        ) != "query":

            print(
                "Mensagem ignorada: "
                "tipo diferente de query."
            )

            return

        # ----------------------------------------------------
        # VALIDA SCHEMA
        # ----------------------------------------------------

        if mensagem.get(
            "schema"
        ) != "ods.codigos.i6":

            print(
                "Mensagem ignorada: "
                "schema desconhecido."
            )

            return

        # ----------------------------------------------------
        # PAYLOAD
        # ----------------------------------------------------

        payload = mensagem.get(
            "payload",
            {}
        )

        request_id = payload.get(
            "request_id"
        )

        camera_id = payload.get(
            "camera_id"
        )

        print(
            f"\nRequest ID: {request_id}"
        )

        print(
            f"Camera ID: {camera_id}"
        )

        # ----------------------------------------------------
        # VALIDA REQUEST ID
        # ----------------------------------------------------

        if not request_id:

            print(
                "Query inválida: "
                "request_id ausente."
            )

            return

        # ====================================================
        # OBTÉM O FRAME
        # ====================================================

        print(
            "\nObtendo imagem para análise..."
        )

        imagem = obter_imagem()

        print(
            "Imagem obtida!"
        )

        # ====================================================
        # ANALISA A IMAGEM
        # ====================================================

        print(
            "\nAnalisando imagem..."
        )

        resultado = analisar_frame(
            imagem
        )

        print(
            "Imagem analisada!"
        )

        # ====================================================
        # MONTA PAYLOAD DA RESPOSTA
        # ====================================================

        payload_resposta = {

            "request_id":
                request_id,

            "timestamp":
                timestamp_atual(),

            "blur_score":
                resultado["blur_score"],

            "noise_score":
                resultado["noise_score"],

            "illumination":
                resultado["illumination"],

            "brightness_mean":
                resultado["brightness_mean"],

            "brightness_std":
                resultado["brightness_std"],

            "dark_pixels_percentage":
                resultado["dark_pixels_percentage"],

            "bright_pixels_percentage":
                resultado["bright_pixels_percentage"]
        }

        # Camera ID é opcional
        if camera_id is not None:

            payload_resposta[
                "camera_id"
            ] = camera_id

        # ====================================================
        # MONTA MENSAGEM MQTT
        # ====================================================

        resposta = {

            "message_type":
                "reply",

            "schema":
                "ods.codigos.i6",

            "schema_version":
                "1.0",

            "producer":
                "I6",

            "published_at":
                timestamp_atual(),

            "payload":
                payload_resposta
        }

        # ====================================================
        # PUBLICA RESPOSTA
        # ====================================================

        info = client.publish(

            TOPICO_REPLY,

            json.dumps(
                resposta,
                ensure_ascii=False
            ),

            qos=1
        )

        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            print(
                "\nResposta enviada!"
            )
            print(
                json.dumps(
                    resposta,
                    indent=4,
                    ensure_ascii=False
                )
            )
        else:
            print(
                f"\nFalha ao publicar resposta. "
                f"Código: {info.rc}"
            )

    except json.JSONDecodeError as erro:
        print(
            "\nERRO: A mensagem recebida "
            "não é um JSON válido."
        )
        print(
            f"Detalhes: {erro}"
        )

    except Exception as erro:
        print(
            "\nErro ao processar Query:"
        )
        print(
            erro
        )



# Se webcam estiver habilitada, inicia a captura antes do MQTT.
if USAR_CAMERA:

    thread_webcam = threading.Thread(
        target=capturar_camera,
        daemon=True
    )

    thread_webcam.start()

    # Dá tempo para a câmera
    # capturar alguns frames.

    time.sleep(2)


# CLIENTE MQTT
client = mqtt.Client(

    callback_api_version=
        mqtt.CallbackAPIVersion.VERSION2,

    client_id="I6"
)

client.on_connect = ao_conectar

client.on_message = ao_receber_mensagem

# CONEXÃO MQTT
print(
    "Conectando ao Mosquitto..."
)

client.connect(
    BROKER,
    PORTA
)

client.loop_forever()