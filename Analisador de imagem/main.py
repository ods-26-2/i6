"""Integra o Analisador de Imagem ao stream TCP do P4 e publica eventos no Kafka.

O framing TCP e o contrato ods.p4.frame seguem a implementação de referência
DetectorQR/leitor.py. A compatibilidade definitiva depende da confirmação da equipe P4.
"""

import json
import os
import socket
import struct
import time
import uuid
from datetime import datetime, timezone

import cv2
import numpy as np
from confluent_kafka import Producer

from Image_Analyzer import analisar_frame


# -----------------------------------------------------------------------------
# Configuração por variáveis de ambiente
# -----------------------------------------------------------------------------

def ler_bool(nome, padrao=False):
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    valor = valor.strip().lower()
    if valor in ("1", "true", "yes", "sim", "on"):
        return True
    if valor in ("0", "false", "no", "nao", "não", "off"):
        return False
    raise ValueError(f"{nome} deve ser true ou false. Valor recebido: {valor}")


MODO_TESTE = ler_bool("I6_TEST_MODE", False)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "34.66.235.91:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ods.i6.image_analysis")
SCHEMA = "ods.i6.image_analysis"
SCHEMA_VERSION = "1.0"
PRODUCER_ID = "I6"
ARTIFACT_VERSION = os.getenv("I6_ARTIFACT_VERSION", "i6-image-analyzer-1.0")

P4_HOST = os.getenv("P4_HOST", "p4")
P4_PORT = int(os.getenv("P4_PORT", "5000"))
P4_CONNECT_RETRY_SECONDS = float(os.getenv("P4_CONNECT_RETRY_SECONDS", "2.0"))
P4_MAX_METADATA_BYTES = 64 * 1024
P4_MAX_FRAME_BYTES = 64 * 1024 * 1024

# Limite de análises por segundo. 0 = analisar todos os frames recebidos.
MAX_ANALYSIS_FPS = float(os.getenv("I6_MAX_ANALYSIS_FPS", "5.0"))

TEST_IMAGE_PATH = os.getenv("I6_TEST_IMAGE_PATH", "/app/imagemdeteste.jpg")
TEST_SOURCE_ID = os.getenv("I6_TEST_SOURCE_ID", "image-analyzer-test")

PIXEL_FORMATS_VALIDOS = {"NV12", "RGB", "BGR", "GRAY"}
producer = None
sequence = 0
ultimo_frame_analisado = {}


def agora_iso_utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validar_iso_utc(valor, nome):
    if not isinstance(valor, str) or not valor.endswith("Z"):
        raise ValueError(f"{nome} deve ser ISO 8601 em UTC e terminar com Z.")
    try:
        datetime.fromisoformat(valor[:-1] + "+00:00")
    except ValueError as erro:
        raise ValueError(f"{nome} não contém uma data ISO 8601 válida.") from erro


def validar_uuid(valor, nome):
    try:
        uuid.UUID(str(valor))
    except (ValueError, TypeError, AttributeError) as erro:
        raise ValueError(f"{nome} deve ser um UUID válido.") from erro


# -----------------------------------------------------------------------------
# Recepção e conversão do frame P4: mesma estrutura do Detector QR
# -----------------------------------------------------------------------------

def validar_metadados_p4(evento):
    if not isinstance(evento, dict):
        raise ValueError("Os metadados do P4 devem ser um objeto JSON.")
    if evento.get("schema") != "ods.p4.frame":
        raise ValueError("schema inválido. Esperado: ods.p4.frame")
    if evento.get("producer") != "P4":
        raise ValueError("producer inválido. Esperado: P4")

    validar_iso_utc(evento.get("captured_at"), "captured_at")
    source_id = evento.get("source_id")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source_id deve ser uma string não vazia.")
    validar_uuid(evento.get("session_id"), "session_id")

    payload = evento.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("payload deve ser um objeto.")

    frame_index = payload.get("frame_index")
    if isinstance(frame_index, bool) or not isinstance(frame_index, int) or frame_index < 0:
        raise ValueError("payload.frame_index deve ser inteiro >= 0.")

    largura = payload.get("image_width")
    altura = payload.get("image_height")
    if isinstance(largura, bool) or not isinstance(largura, int) or largura <= 0:
        raise ValueError("payload.image_width deve ser inteiro > 0.")
    if isinstance(altura, bool) or not isinstance(altura, int) or altura <= 0:
        raise ValueError("payload.image_height deve ser inteiro > 0.")

    pixel_format = payload.get("pixel_format")
    if pixel_format not in PIXEL_FORMATS_VALIDOS:
        raise ValueError("payload.pixel_format deve ser NV12, RGB, BGR ou GRAY.")

    fps = payload.get("fps")
    if isinstance(fps, bool) or not isinstance(fps, (int, float)) or fps <= 0:
        raise ValueError("payload.fps deve ser número > 0.")
    if payload.get("raw") is not True:
        raise ValueError("payload.raw deve ser true.")

    return {
        "captured_at": evento["captured_at"],
        "source_id": source_id.strip(),
        "session_id": str(uuid.UUID(evento["session_id"])),
        "frame_index": frame_index,
        "image_width": largura,
        "image_height": altura,
        "pixel_format": pixel_format,
        "fps": float(fps),
    }


def tamanho_buffer_esperado(largura, altura, pixel_format):
    pixels = largura * altura
    if pixel_format == "NV12":
        if largura % 2 or altura % 2:
            raise ValueError("NV12 exige largura e altura pares.")
        return pixels * 3 // 2
    if pixel_format in ("RGB", "BGR"):
        return pixels * 3
    if pixel_format == "GRAY":
        return pixels
    raise ValueError(f"Formato não suportado: {pixel_format}")


def converter_buffer_para_bgr(buffer_bruto, largura, altura, pixel_format):
    esperado = tamanho_buffer_esperado(largura, altura, pixel_format)
    if len(buffer_bruto) != esperado:
        raise ValueError(f"Tamanho do buffer incompatível. Esperado={esperado}, recebido={len(buffer_bruto)}")
    array = np.frombuffer(buffer_bruto, dtype=np.uint8)

    if pixel_format == "BGR":
        return array.reshape(altura, largura, 3).copy()
    if pixel_format == "RGB":
        rgb = array.reshape(altura, largura, 3)
        return cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
    if pixel_format == "GRAY":
        gray = array.reshape(altura, largura)
        return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
    if pixel_format == "NV12":
        nv12 = array.reshape(altura * 3 // 2, largura)
        return cv2.cvtColor(nv12, cv2.COLOR_YUV2BGR_NV12)
    raise ValueError(f"Formato não suportado: {pixel_format}")


def receber_exatamente(conexao, quantidade):
    dados = bytearray()
    while len(dados) < quantidade:
        parte = conexao.recv(quantidade - len(dados))
        if not parte:
            raise ConnectionError("Conexão com o P4 foi encerrada.")
        dados.extend(parte)
    return bytes(dados)


def receber_frame_p4(conexao):
    # Protocolo de framing documentado pelo Detector QR: uint32 + JSON + uint64 + buffer.
    tamanho_metadata = struct.unpack("!I", receber_exatamente(conexao, 4))[0]
    if tamanho_metadata <= 0 or tamanho_metadata > P4_MAX_METADATA_BYTES:
        raise ValueError(f"Tamanho dos metadados P4 inválido: {tamanho_metadata} bytes.")

    metadata_bytes = receber_exatamente(conexao, tamanho_metadata)
    try:
        evento = json.loads(metadata_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        raise ValueError("Metadados P4 não são JSON UTF-8 válido.") from erro

    metadados = validar_metadados_p4(evento)
    tamanho_frame = struct.unpack("!Q", receber_exatamente(conexao, 8))[0]
    if tamanho_frame <= 0 or tamanho_frame > P4_MAX_FRAME_BYTES:
        raise ValueError(f"Tamanho do frame P4 inválido: {tamanho_frame} bytes.")

    esperado = tamanho_buffer_esperado(
        metadados["image_width"], metadados["image_height"], metadados["pixel_format"]
    )
    if tamanho_frame != esperado:
        raise ValueError(f"Tamanho do frame incompatível. Esperado={esperado}, informado={tamanho_frame}.")

    buffer_bruto = receber_exatamente(conexao, tamanho_frame)
    frame = converter_buffer_para_bgr(
        buffer_bruto, metadados["image_width"], metadados["image_height"], metadados["pixel_format"]
    )
    return frame, metadados


# -----------------------------------------------------------------------------
# Controle de frequência e publicação Kafka
# -----------------------------------------------------------------------------

def deve_analisar_frame(source_id):
    """Limita a taxa de análise. Frames excedentes são descartados antes do OpenCV."""
    if MAX_ANALYSIS_FPS == 0:
        return True

    intervalo_minimo = 1.0 / MAX_ANALYSIS_FPS
    agora = time.monotonic()
    ultimo = ultimo_frame_analisado.get(source_id)
    if ultimo is not None and (agora - ultimo) < intervalo_minimo:
        return False

    ultimo_frame_analisado[source_id] = agora
    return True


def callback_kafka(erro, mensagem):
    if erro is not None:
        print(f"[Kafka] falha na entrega: {erro}")
    else:
        print(
            f"[Kafka] entrega confirmada: topic={mensagem.topic()}, "
            f"partition={mensagem.partition()}, offset={mensagem.offset()}"
        )


def iniciar_kafka():
    global producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
    })
    print("[Kafka] produtor pronto.")
    print(f"[Kafka] tópico: {KAFKA_TOPIC}")


def montar_evento(metadados, resultado):
    global sequence
    sequence += 1

    payload = {
        "frame_index": metadados["frame_index"],
        "image_width": metadados["image_width"],
        "image_height": metadados["image_height"],
        "pixel_format": metadados["pixel_format"],
        "source_fps": metadados["fps"],
        "blur_score": resultado["blur_score"],
        "noise_score": resultado["noise_score"],
        "illumination": resultado["illumination"],
        "brightness_mean": resultado["brightness_mean"],
        "brightness_std": resultado["brightness_std"],
        "dark_pixels_percentage": resultado["dark_pixels_percentage"],
        "bright_pixels_percentage": resultado["bright_pixels_percentage"],
        "pixel_distribution": resultado["pixel_distribution"],
        "pixel_distribution_percentage": resultado["pixel_distribution_percentage"],
    }

    return {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_ID,
        "published_at": agora_iso_utc(),
        "captured_at": metadados["captured_at"],
        "source_id": metadados["source_id"],
        "session_id": metadados["session_id"],
        "sequence": sequence,
        "artifact_version": ARTIFACT_VERSION,
        "payload": payload,
    }


def publicar_resultado(metadados, resultado):
    evento = montar_evento(metadados, resultado)
    valor = json.dumps(evento, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=metadados["source_id"].encode("utf-8"),
        value=valor,
        callback=callback_kafka,
    )
    producer.poll(0)
    print(
        f"[I6] análise publicada: source_id={metadados['source_id']}, "
        f"frame_index={metadados['frame_index']}"
    )


def processar_frame(frame, metadados):
    if not deve_analisar_frame(metadados["source_id"]):
        return False
    resultado = analisar_frame(frame)
    publicar_resultado(metadados, resultado)
    return True


# -----------------------------------------------------------------------------
# Modo produção: conexão TCP com o P4
# -----------------------------------------------------------------------------

def executar_modo_p4():
    print("[I6] MODO PRODUÇÃO - Analisador de Imagem")
    print(f"[P4] tentando conectar em {P4_HOST}:{P4_PORT}")
    print(f"[I6] limite de análise: {MAX_ANALYSIS_FPS:g} FPS (0 = todos os frames)")

    while True:
        conexao = None
        try:
            conexao = socket.create_connection((P4_HOST, P4_PORT), timeout=5.0)
            conexao.settimeout(None)
            print("[P4] conectado. Aguardando frames...")

            while True:
                frame, metadados = receber_frame_p4(conexao)
                processar_frame(frame, metadados)

        except ValueError as erro:
            print(f"[P4] dados inválidos recebidos: {erro}")
            time.sleep(P4_CONNECT_RETRY_SECONDS)
        except (ConnectionError, OSError) as erro:
            print(f"[P4] conexão indisponível/perdida: {erro}")
            print(f"[P4] nova tentativa em {P4_CONNECT_RETRY_SECONDS}s.")
            time.sleep(P4_CONNECT_RETRY_SECONDS)
        finally:
            if conexao is not None:
                try:
                    conexao.close()
                except OSError:
                    pass


# -----------------------------------------------------------------------------
# Modo de teste: analisa uma imagem local repetidamente para testar Kafka.
# -----------------------------------------------------------------------------

def executar_modo_teste():
    imagem = cv2.imread(TEST_IMAGE_PATH)
    if imagem is None:
        raise FileNotFoundError(f"Não foi possível carregar a imagem de teste: {TEST_IMAGE_PATH}")

    altura, largura = imagem.shape[:2]
    session_id = str(uuid.uuid4())
    frame_index = 0
    print(f"[TESTE] imagem carregada: {TEST_IMAGE_PATH} ({largura}x{altura})")
    print(f"[I6] limite de análise: {MAX_ANALYSIS_FPS:g} FPS (0 = todos os frames)")

    while True:
        metadados = {
            "captured_at": agora_iso_utc(),
            "source_id": TEST_SOURCE_ID,
            "session_id": session_id,
            "frame_index": frame_index,
            "image_width": largura,
            "image_height": altura,
            "pixel_format": "BGR",
            "fps": MAX_ANALYSIS_FPS if MAX_ANALYSIS_FPS > 0 else 1.0,
        }
        processar_frame(imagem, metadados)
        frame_index += 1
        time.sleep(0.001 if MAX_ANALYSIS_FPS == 0 else 1.0 / MAX_ANALYSIS_FPS)


def validar_configuracao():
    if not KAFKA_BOOTSTRAP_SERVERS.strip():
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS não pode ser vazio.")
    if not KAFKA_TOPIC.strip():
        raise ValueError("KAFKA_TOPIC não pode ser vazio.")
    if MAX_ANALYSIS_FPS < 0:
        raise ValueError("I6_MAX_ANALYSIS_FPS deve ser >= 0. Use 0 para analisar todos os frames.")
    if P4_PORT < 1 or P4_PORT > 65535:
        raise ValueError("P4_PORT deve estar entre 1 e 65535.")
    if P4_CONNECT_RETRY_SECONDS <= 0:
        raise ValueError("P4_CONNECT_RETRY_SECONDS deve ser > 0.")


def main():
    validar_configuracao()
    iniciar_kafka()
    try:
        if MODO_TESTE:
            executar_modo_teste()
        else:
            executar_modo_p4()
    except KeyboardInterrupt:
        print("\n[I6] encerramento solicitado.")
    finally:
        if producer is not None:
            producer.flush(10)
            print("[Kafka] produtor encerrado.")


if __name__ == "__main__":
    main()
