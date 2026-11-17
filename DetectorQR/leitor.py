import os
import cv2
import json
import time
import uuid
import threading
import socket
import struct
from datetime import datetime, timezone
from collections import defaultdict

import numpy as np
from pyzbar.pyzbar import decode
from confluent_kafka import Producer


# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

def ler_bool(nome, padrao=False):
    valor = os.getenv(nome)
    if valor is None:
        return padrao

    valor = valor.strip().lower()

    if valor in ("1", "true", "yes", "sim", "on"):
        return True

    if valor in ("0", "false", "no", "nao", "não", "off"):
        return False

    raise ValueError(
        f"{nome} deve ser true ou false. Valor recebido: {valor}"
    )


MODO_TESTE = ler_bool("I6_TEST_MODE", False)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "34.66.235.91:9093"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ods.i6.code_read"
)

SCHEMA = "ods.i6.code_read"
SCHEMA_VERSION = "1.0"
PRODUCER_ID = "I6"

ARTIFACT_VERSION = os.getenv(
    "I6_ARTIFACT_VERSION",
    "i6-qrcode-reader-2.0"
)

# ------------------------------------------------------------
# Configuração do P4
# ------------------------------------------------------------
#
# Foi informado que a integração P4 -> I6 provavelmente será
# feita por socket TCP. Por isso o I6 fica preparado como
# CLIENTE TCP.
#
# O protocolo exato do P4 ainda precisa ser confirmado.
# Enquanto isso, o I6 espera este framing simples:
#
#   4 bytes  -> tamanho do JSON ods.p4.frame
#   N bytes  -> JSON ods.p4.frame
#   8 bytes  -> tamanho do buffer da imagem
#   M bytes  -> buffer bruto do frame
#
# Se o P4 usar outro framing, basta ajustar receber_frame_p4().
# A detecção, Kafka e lógica multi-câmera continuam iguais.
#
P4_HOST = os.getenv(
    "P4_HOST",
    "p4"
)

P4_PORT = int(
    os.getenv(
        "P4_PORT",
        "5000"
    )
)

P4_CONNECT_RETRY_SECONDS = float(
    os.getenv(
        "P4_CONNECT_RETRY_SECONDS",
        "2.0"
    )
)

# Proteções contra tamanhos corrompidos recebidos pelo socket.
P4_MAX_METADATA_BYTES = 64 * 1024
P4_MAX_FRAME_BYTES = 64 * 1024 * 1024

# ------------------------------------------------------------
# Câmeras usadas SOMENTE em modo de teste
# ------------------------------------------------------------
#
# Exemplo:
# I6_TEST_CAMERAS_JSON=[
#   {"source_id":"cam0","url":"http://192.168.0.10:8080/video"},
#   {"source_id":"cam1","url":"http://192.168.0.11:8080/video"}
# ]
#
TEST_CAMERAS_JSON = os.getenv(
    "I6_TEST_CAMERAS_JSON",
    "[]"
)

TEST_RECONNECT_SECONDS = float(
    os.getenv("I6_TEST_RECONNECT_SECONDS", "2.0")
)

# ------------------------------------------------------------
# Estabilidade da leitura
# ------------------------------------------------------------

ACQUIRE_CONFIRM_FRAMES = int(
    os.getenv("I6_ACQUIRE_CONFIRM_FRAMES", "4")
)

SWITCH_CONFIRM_FRAMES = int(
    os.getenv("I6_SWITCH_CONFIRM_FRAMES", "10")
)

SWITCH_AREA_RATIO = float(
    os.getenv("I6_SWITCH_AREA_RATIO", "1.5")
)

QR_LOST_SECONDS = float(
    os.getenv("I6_QR_LOST_SECONDS", "3.0")
)

# O contrato diz que o mesmo código parado não deve ser
# republicado mais de 1 vez por segundo.
# Mantemos 3 segundos, como no projeto anterior.
REPEAT_COOLDOWN_SECONDS = float(
    os.getenv("I6_REPEAT_COOLDOWN_SECONDS", "3.0")
)


# ============================================================
# 2. FUNÇÕES AUXILIARES
# ============================================================

def agora_iso_utc():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def validar_iso_utc(valor, nome):
    if not isinstance(valor, str) or not valor.endswith("Z"):
        raise ValueError(
            f"{nome} deve ser uma string ISO 8601 em UTC terminando em Z."
        )

    try:
        datetime.fromisoformat(
            valor[:-1] + "+00:00"
        )
    except ValueError as erro:
        raise ValueError(
            f"{nome} não contém uma data ISO 8601 válida."
        ) from erro


def validar_uuid(valor, nome):
    try:
        uuid.UUID(str(valor))
    except (ValueError, TypeError, AttributeError) as erro:
        raise ValueError(
            f"{nome} deve ser um UUID válido."
        ) from erro


def area_bbox(bbox):
    x1, y1, x2, y2 = bbox

    return max(0, x2 - x1) * max(
        0,
        y2 - y1
    )


# ============================================================
# 3. VALIDAÇÃO DO CONTRATO ods.p4.frame
# ============================================================

PIXEL_FORMATS_VALIDOS = {
    "NV12",
    "RGB",
    "BGR",
    "GRAY"
}


def validar_metadados_p4(evento):
    if not isinstance(evento, dict):
        raise ValueError(
            "Os metadados do P4 devem ser um objeto JSON."
        )

    if evento.get("schema") != "ods.p4.frame":
        raise ValueError(
            "schema inválido. Esperado: ods.p4.frame"
        )

    # Em captura ao vivo o produtor esperado é P4.
    # P6 pode usar o mesmo formato em replay, mas esse I6
    # está configurado para integração direta com P4.
    if evento.get("producer") != "P4":
        raise ValueError(
            "producer inválido. Esperado: P4"
        )

    validar_iso_utc(
        evento.get("captured_at"),
        "captured_at"
    )

    source_id = evento.get("source_id")

    if (
        not isinstance(source_id, str)
        or not source_id.strip()
    ):
        raise ValueError(
            "source_id deve ser uma string não vazia."
        )

    validar_uuid(
        evento.get("session_id"),
        "session_id"
    )

    payload = evento.get("payload")

    if not isinstance(payload, dict):
        raise ValueError(
            "payload deve ser um objeto."
        )

    frame_index = payload.get("frame_index")

    if (
        isinstance(frame_index, bool)
        or not isinstance(frame_index, int)
        or frame_index < 0
    ):
        raise ValueError(
            "payload.frame_index deve ser inteiro >= 0."
        )

    largura = payload.get("image_width")
    altura = payload.get("image_height")

    if (
        isinstance(largura, bool)
        or not isinstance(largura, int)
        or largura <= 0
    ):
        raise ValueError(
            "payload.image_width deve ser inteiro > 0."
        )

    if (
        isinstance(altura, bool)
        or not isinstance(altura, int)
        or altura <= 0
    ):
        raise ValueError(
            "payload.image_height deve ser inteiro > 0."
        )

    pixel_format = payload.get(
        "pixel_format"
    )

    if pixel_format not in PIXEL_FORMATS_VALIDOS:
        raise ValueError(
            "payload.pixel_format inválido. "
            "Permitidos: NV12, RGB, BGR e GRAY."
        )

    fps = payload.get("fps")

    if (
        isinstance(fps, bool)
        or not isinstance(fps, (int, float))
        or fps <= 0
    ):
        raise ValueError(
            "payload.fps deve ser número > 0."
        )

    if payload.get("raw") is not True:
        raise ValueError(
            "payload.raw deve ser true."
        )

    return {
        "captured_at": evento["captured_at"],
        "source_id": source_id.strip(),
        "session_id": str(
            uuid.UUID(evento["session_id"])
        ),
        "frame_index": frame_index,
        "image_width": largura,
        "image_height": altura,
        "pixel_format": pixel_format,
        "fps": float(fps)
    }


def tamanho_buffer_esperado(
    largura,
    altura,
    pixel_format
):
    pixels = largura * altura

    if pixel_format == "NV12":
        if (
            largura % 2 != 0
            or altura % 2 != 0
        ):
            raise ValueError(
                "NV12 exige largura e altura pares."
            )

        return pixels * 3 // 2

    if pixel_format in ("RGB", "BGR"):
        return pixels * 3

    if pixel_format == "GRAY":
        return pixels

    raise ValueError(
        f"Formato não suportado: {pixel_format}"
    )


def converter_buffer_para_bgr(
    buffer_bruto,
    largura,
    altura,
    pixel_format
):
    esperado = tamanho_buffer_esperado(
        largura,
        altura,
        pixel_format
    )

    recebido = len(buffer_bruto)

    if recebido != esperado:
        raise ValueError(
            "Tamanho do buffer incompatível. "
            f"Esperado={esperado}, recebido={recebido}"
        )

    array = np.frombuffer(
        buffer_bruto,
        dtype=np.uint8
    )

    if pixel_format == "BGR":
        return array.reshape(
            altura,
            largura,
            3
        ).copy()

    if pixel_format == "RGB":
        rgb = array.reshape(
            altura,
            largura,
            3
        )

        return cv2.cvtColor(
            rgb,
            cv2.COLOR_RGB2BGR
        )

    if pixel_format == "GRAY":
        gray = array.reshape(
            altura,
            largura
        )

        return cv2.cvtColor(
            gray,
            cv2.COLOR_GRAY2BGR
        )

    if pixel_format == "NV12":
        nv12 = array.reshape(
            altura * 3 // 2,
            largura
        )

        return cv2.cvtColor(
            nv12,
            cv2.COLOR_YUV2BGR_NV12
        )

    raise ValueError(
        f"Formato não suportado: {pixel_format}"
    )


# ============================================================
# 4. DETECÇÃO E DECODIFICAÇÃO
# ============================================================

TIPOS_SUPORTADOS = {
    "QRCODE": "QR_CODE",
    "CODE128": "CODE_128",
    "EAN13": "EAN_13"
}


def detectar_e_decodificar(frame):
    # Mantemos a estratégia já usada no projeto:
    # canal azul + binarização de Otsu.
    canal_azul = frame[:, :, 0]

    _, binarizada = cv2.threshold(
        canal_azul,
        0,
        255,
        cv2.THRESH_BINARY
        | cv2.THRESH_OTSU
    )

    objetos = decode(binarizada)

    resultados = []

    for obj in objetos:
        code_type = TIPOS_SUPORTADOS.get(
            obj.type
        )

        # O contrato do I6 1.0 suporta somente
        # QR_CODE, CODE_128 e EAN_13.
        if code_type is None:
            continue

        try:
            # O contrato pede "conteúdo decodificado,
            # sem interpretação".
            decoded_data = obj.data.decode(
                "utf-8"
            )
        except UnicodeDecodeError:
            # O contrato atual exige string.
            # Não inventamos Base64 aqui.
            continue

        if not decoded_data:
            continue

        x, y, w, h = obj.rect

        x1 = max(0, int(x))
        y1 = max(0, int(y))
        x2 = min(
            frame.shape[1],
            int(x + w)
        )
        y2 = min(
            frame.shape[0],
            int(y + h)
        )

        if x2 <= x1 or y2 <= y1:
            continue

        resultados.append(
            {
                "code_type": code_type,
                "decoded_data": decoded_data,
                "bbox": [
                    x1,
                    y1,
                    x2,
                    y2
                ],
                "area": area_bbox(
                    [x1, y1, x2, y2]
                )
            }
        )

    return resultados


# ============================================================
# 5. ESTADO DE LEITURA POR CÂMERA
# ============================================================

#
# Cada source_id possui estado independente.
#
# Exemplo:
#
# estados["cam0"] -> QR ativo, cooldown, frame_index...
# estados["cam1"] -> outro estado completamente separado
#
estados = {}


def criar_estado(session_id):
    return {
        "session_id": session_id,
        "ultimo_frame_index": -1,

        "codigo_ativo": None,
        "area_ativa": 0,

        "candidato": None,
        "candidato_contagem": 0,

        "ultima_vez_visto": 0.0,
        "ultima_publicacao": 0.0
    }


def obter_estado(
    source_id,
    session_id
):
    estado = estados.get(source_id)

    # Nova câmera ou nova sessão do P4:
    # reinicia apenas o estado daquela fonte.
    if (
        estado is None
        or estado["session_id"]
        != session_id
    ):
        estado = criar_estado(
            session_id
        )

        estados[source_id] = estado

        print(
            f"[I6] estado criado/reiniciado: "
            f"source_id={source_id}, "
            f"session_id={session_id}"
        )

    return estado


def validar_ordem_frame(
    estado,
    frame_index
):
    if (
        frame_index
        <= estado["ultimo_frame_index"]
    ):
        return False

    estado["ultimo_frame_index"] = (
        frame_index
    )

    return True


def chave_codigo(leitura):
    return (
        leitura["code_type"],
        leitura["decoded_data"]
    )


def limpar_candidato(estado):
    estado["candidato"] = None
    estado["candidato_contagem"] = 0


def avancar_candidato(
    estado,
    leitura
):
    chave = chave_codigo(
        leitura
    )

    if estado["candidato"] == chave:
        estado["candidato_contagem"] += 1
    else:
        estado["candidato"] = chave
        estado["candidato_contagem"] = 1

    return estado["candidato_contagem"]


def escolher_leitura_estavel(
    source_id,
    session_id,
    frame_index,
    captured_at,
    leituras
):
    agora = time.monotonic()

    estado = obter_estado(
        source_id,
        session_id
    )

    if not leituras:
        if (
            estado["codigo_ativo"] is not None
            and agora
            - estado["ultima_vez_visto"]
            >= QR_LOST_SECONDS
        ):
            estado["codigo_ativo"] = None
            estado["area_ativa"] = 0
            estado["ultima_publicacao"] = 0.0

            limpar_candidato(
                estado
            )

        return None

    # Mantemos o código visualmente predominante.
    leitura = max(
        leituras,
        key=lambda item: item["area"]
    )

    chave = chave_codigo(
        leitura
    )

    estado["ultima_vez_visto"] = (
        agora
    )

    # --------------------------------------------------------
    # Nenhum código ativo ainda
    # --------------------------------------------------------

    if estado["codigo_ativo"] is None:
        contagem = avancar_candidato(
            estado,
            leitura
        )

        if (
            contagem
            < ACQUIRE_CONFIRM_FRAMES
        ):
            return None

        estado["codigo_ativo"] = chave
        estado["area_ativa"] = leitura["area"]
        estado["ultima_publicacao"] = agora

        limpar_candidato(
            estado
        )

        return leitura

    # --------------------------------------------------------
    # Continua sendo o mesmo código
    # --------------------------------------------------------

    if chave == estado["codigo_ativo"]:
        estado["area_ativa"] = leitura["area"]

        limpar_candidato(
            estado
        )

        if (
            agora
            - estado["ultima_publicacao"]
            >= REPEAT_COOLDOWN_SECONDS
        ):
            estado["ultima_publicacao"] = (
                agora
            )

            return leitura

        return None

    # --------------------------------------------------------
    # Apareceu outro código
    # --------------------------------------------------------

    area_minima = (
        max(
            estado["area_ativa"],
            1
        )
        * SWITCH_AREA_RATIO
    )

    # O novo código só começa a substituir o atual
    # quando tiver área suficiente.
    if leitura["area"] < area_minima:
        limpar_candidato(
            estado
        )

        return None

    contagem = avancar_candidato(
        estado,
        leitura
    )

    if (
        contagem
        < SWITCH_CONFIRM_FRAMES
    ):
        return None

    estado["codigo_ativo"] = chave
    estado["area_ativa"] = leitura["area"]
    estado["ultima_publicacao"] = agora

    limpar_candidato(
        estado
    )

    return leitura


# ============================================================
# 6. KAFKA - ods.i6.code_read
# ============================================================

producer = None

#
# sequence é crescente por producer + session_id.
#
sequencia_por_sessao = defaultdict(int)
lock_kafka = threading.Lock()


def callback_kafka(
    erro,
    mensagem
):
    if erro is not None:
        print(
            f"[Kafka] erro de entrega: {erro}"
        )

        return

    print(
        "[Kafka] entrega confirmada: "
        f"topic={mensagem.topic()}, "
        f"partition={mensagem.partition()}, "
        f"offset={mensagem.offset()}"
    )


def iniciar_kafka():
    global producer

    producer = Producer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            # Garante segurança contra duplicação
            # causada pelo próprio producer em retries.
            "enable.idempotence": True,
            "acks": "all"
        }
    )

    print(
        "[Kafka] produtor pronto."
    )

    print(
        f"[Kafka] tópico: {KAFKA_TOPIC}"
    )


def montar_evento_i6(
    source_id,
    session_id,
    captured_at,
    frame_index,
    leitura
):
    with lock_kafka:
        sequence = (
            sequencia_por_sessao[
                session_id
            ]
        )

        sequencia_por_sessao[
            session_id
        ] += 1

    evento = {
        "message_type": "event",
        "message_id": str(
            uuid.uuid4()
        ),
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_ID,
        "published_at": agora_iso_utc(),

        # Proveniência recebida do P4
        "captured_at": captured_at,
        "source_id": source_id,
        "session_id": session_id,

        "sequence": sequence,
        "artifact_version":
            ARTIFACT_VERSION,

        "payload": {
            "frame_index": frame_index,
            "code_type":
                leitura["code_type"],
            "decoded_data":
                leitura["decoded_data"],
            "bbox":
                leitura["bbox"]
        }
    }

    return evento


def validar_evento_i6(evento):
    campos_obrigatorios = {
        "message_type",
        "message_id",
        "schema",
        "schema_version",
        "producer",
        "published_at",
        "captured_at",
        "source_id",
        "session_id",
        "sequence",
        "artifact_version",
        "payload"
    }

    faltando = (
        campos_obrigatorios
        - set(evento.keys())
    )

    if faltando:
        raise ValueError(
            "Evento I6 inválido. "
            f"Campos ausentes: {sorted(faltando)}"
        )

    if evento["message_type"] != "event":
        raise ValueError(
            "message_type deve ser event."
        )

    validar_uuid(
        evento["message_id"],
        "message_id"
    )

    if evento["schema"] != SCHEMA:
        raise ValueError(
            "schema de saída inválido."
        )

    if (
        evento["schema_version"]
        != SCHEMA_VERSION
    ):
        raise ValueError(
            "schema_version inválido."
        )

    if evento["producer"] != "I6":
        raise ValueError(
            "producer deve ser I6."
        )

    validar_iso_utc(
        evento["published_at"],
        "published_at"
    )

    validar_iso_utc(
        evento["captured_at"],
        "captured_at"
    )

    validar_uuid(
        evento["session_id"],
        "session_id"
    )

    payload = evento["payload"]

    if set(payload.keys()) != {
        "frame_index",
        "code_type",
        "decoded_data",
        "bbox"
    }:
        raise ValueError(
            "payload de ods.i6.code_read "
            "está fora do contrato."
        )

    if payload["code_type"] not in {
        "QR_CODE",
        "CODE_128",
        "EAN_13"
    }:
        raise ValueError(
            "code_type inválido."
        )

    bbox = payload["bbox"]

    if (
        not isinstance(bbox, list)
        or len(bbox) != 4
        or not all(
            isinstance(v, int)
            for v in bbox
        )
    ):
        raise ValueError(
            "bbox deve ter quatro inteiros."
        )


def publicar_leitura(
    source_id,
    session_id,
    captured_at,
    frame_index,
    leitura
):
    evento = montar_evento_i6(
        source_id,
        session_id,
        captured_at,
        frame_index,
        leitura
    )

    # Valida antes de publicar.
    validar_evento_i6(
        evento
    )

    valor = json.dumps(
        evento,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    producer.produce(
        topic=KAFKA_TOPIC,
        key=source_id.encode(
            "utf-8"
        ),
        value=valor,
        callback=callback_kafka
    )

    producer.poll(0)

    print(
        "[I6] leitura publicada:"
    )

    print(
        json.dumps(
            evento,
            indent=2,
            ensure_ascii=False
        )
    )


# ============================================================
# 7. PROCESSAMENTO COMUM
# ============================================================

def processar_frame(
    frame,
    metadados
):
    source_id = metadados[
        "source_id"
    ]

    session_id = metadados[
        "session_id"
    ]

    frame_index = metadados[
        "frame_index"
    ]

    captured_at = metadados[
        "captured_at"
    ]

    estado = obter_estado(
        source_id,
        session_id
    )

    # Protege contra frame repetido ou fora de ordem.
    if not validar_ordem_frame(
        estado,
        frame_index
    ):
        print(
            "[I6] frame repetido/fora de ordem "
            f"descartado: "
            f"source_id={source_id}, "
            f"frame_index={frame_index}"
        )

        return

    leituras = detectar_e_decodificar(
        frame
    )

    leitura = escolher_leitura_estavel(
        source_id,
        session_id,
        frame_index,
        captured_at,
        leituras
    )

    if leitura is None:
        return

    publicar_leitura(
        source_id,
        session_id,
        captured_at,
        frame_index,
        leitura
    )


# ============================================================
# 8. MODO PRODUÇÃO - RECEBE O STREAM DO P4 VIA SOCKET TCP
# ============================================================

def receber_exatamente(conexao, quantidade):
    """
    TCP entrega um fluxo de bytes, não mensagens completas.

    Esta função continua chamando recv() até receber exatamente
    a quantidade pedida.
    """

    dados = bytearray()

    while len(dados) < quantidade:
        parte = conexao.recv(
            quantidade - len(dados)
        )

        if not parte:
            raise ConnectionError(
                "Conexão com o P4 foi encerrada."
            )

        dados.extend(parte)

    return bytes(dados)


def receber_frame_p4(conexao):
    """
    Recebe um frame do P4 por socket TCP.

    Protocolo provisório esperado:

        4 bytes
            tamanho do JSON (uint32, big-endian)

        N bytes
            JSON UTF-8 ods.p4.frame

        8 bytes
            tamanho do buffer da imagem (uint64, big-endian)

        M bytes
            buffer bruto do frame

    Quando a equipe P4 confirmar o protocolo definitivo,
    ajuste esta função se necessário.
    """

    # 1) Tamanho do JSON
    tamanho_metadata = struct.unpack(
        "!I",
        receber_exatamente(
            conexao,
            4
        )
    )[0]

    if (
        tamanho_metadata <= 0
        or tamanho_metadata > P4_MAX_METADATA_BYTES
    ):
        raise ValueError(
            "Tamanho dos metadados P4 inválido: "
            f"{tamanho_metadata} bytes."
        )

    # 2) JSON ods.p4.frame
    metadata_bytes = receber_exatamente(
        conexao,
        tamanho_metadata
    )

    try:
        evento = json.loads(
            metadata_bytes.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError
    ) as erro:
        raise ValueError(
            "Metadados P4 não são JSON UTF-8 válido."
        ) from erro

    metadados = validar_metadados_p4(
        evento
    )

    # 3) Tamanho do buffer do frame
    tamanho_frame = struct.unpack(
        "!Q",
        receber_exatamente(
            conexao,
            8
        )
    )[0]

    if (
        tamanho_frame <= 0
        or tamanho_frame > P4_MAX_FRAME_BYTES
    ):
        raise ValueError(
            "Tamanho do frame P4 inválido: "
            f"{tamanho_frame} bytes."
        )

    # Confere o tamanho com largura, altura e pixel_format
    # declarados no ods.p4.frame.
    tamanho_esperado = tamanho_buffer_esperado(
        metadados["image_width"],
        metadados["image_height"],
        metadados["pixel_format"]
    )

    if tamanho_frame != tamanho_esperado:
        raise ValueError(
            "Tamanho informado pelo socket não bate "
            "com ods.p4.frame. "
            f"Esperado={tamanho_esperado}, "
            f"informado={tamanho_frame}."
        )

    # 4) Buffer do frame
    buffer_bruto = receber_exatamente(
        conexao,
        tamanho_frame
    )

    frame = converter_buffer_para_bgr(
        buffer_bruto,
        metadados["image_width"],
        metadados["image_height"],
        metadados["pixel_format"]
    )

    return frame, metadados


def executar_modo_p4():
    print(
        "[I6] MODO PRODUÇÃO"
    )

    print(
        "[I6] câmera física é responsabilidade do P4."
    )

    print(
        "[I6] recebimento preparado para socket TCP."
    )

    print(
        f"[P4] endereço configurado: "
        f"{P4_HOST}:{P4_PORT}"
    )

    while True:
        conexao = None

        try:
            print(
                f"[P4] tentando conectar em "
                f"{P4_HOST}:{P4_PORT}..."
            )

            conexao = socket.create_connection(
                (
                    P4_HOST,
                    P4_PORT
                ),
                timeout=5.0
            )

            # Depois da conexão, podemos aguardar os frames
            # normalmente sem timeout de leitura.
            conexao.settimeout(None)

            print(
                "[P4] conectado. Aguardando frames..."
            )

            while True:
                frame, metadados = (
                    receber_frame_p4(
                        conexao
                    )
                )

                processar_frame(
                    frame,
                    metadados
                )

        except ValueError as erro:
            print(
                "[P4] dados inválidos recebidos: "
                f"{erro}"
            )

            # Como o framing pode ter perdido sincronismo,
            # reconecta antes de continuar.
            time.sleep(
                P4_CONNECT_RETRY_SECONDS
            )

        except (
            ConnectionError,
            OSError
        ) as erro:
            print(
                "[P4] conexão indisponível/perdida: "
                f"{erro}"
            )

            print(
                f"[P4] nova tentativa em "
                f"{P4_CONNECT_RETRY_SECONDS}s."
            )

            time.sleep(
                P4_CONNECT_RETRY_SECONDS
            )

        finally:
            if conexao is not None:
                try:
                    conexao.close()
                except OSError:
                    pass


# ============================================================
# 9. MODO TESTE - CÂMERAS IP DIRETAS
# ============================================================

def carregar_cameras_teste():
    try:
        cameras = json.loads(
            TEST_CAMERAS_JSON
        )
    except json.JSONDecodeError as erro:
        raise ValueError(
            "I6_TEST_CAMERAS_JSON "
            "não contém JSON válido."
        ) from erro

    if (
        not isinstance(cameras, list)
        or not cameras
    ):
        raise ValueError(
            "No modo teste, "
            "I6_TEST_CAMERAS_JSON deve "
            "conter pelo menos uma câmera."
        )

    ids = set()

    for camera in cameras:
        if not isinstance(
            camera,
            dict
        ):
            raise ValueError(
                "Cada câmera deve ser um objeto."
            )

        source_id = str(
            camera.get(
                "source_id",
                ""
            )
        ).strip()

        url = str(
            camera.get(
                "url",
                ""
            )
        ).strip()

        if not source_id:
            raise ValueError(
                "Câmera sem source_id."
            )

        if not url:
            raise ValueError(
                f"Câmera {source_id} sem url."
            )

        if source_id in ids:
            raise ValueError(
                f"source_id duplicado: {source_id}"
            )

        ids.add(
            source_id
        )

    return cameras


def conectar_camera_teste(camera):
    source_id = camera[
        "source_id"
    ]

    url = camera[
        "url"
    ]

    print(
        f"[TESTE] conectando "
        f"{source_id} -> {url}"
    )

    cap = cv2.VideoCapture(
        url
    )

    cap.set(
        cv2.CAP_PROP_BUFFERSIZE,
        1
    )

    if not cap.isOpened():
        print(
            f"[TESTE] falha ao abrir {source_id}"
        )

        cap.release()

        return None

    print(
        f"[TESTE] {source_id} conectada."
    )

    return cap


def executar_modo_teste():
    print(
        "[I6] MODO TESTE"
    )

    print(
        "[I6] o I6 abrirá câmeras IP diretamente."
    )

    cameras = carregar_cameras_teste()

    # Uma única sessão de teste para todas as fontes
    # desta execução, equivalente à sessão de captura.
    session_id = str(
        uuid.uuid4()
    )

    fontes = {}

    for camera in cameras:
        fontes[
            camera["source_id"]
        ] = {
            "config": camera,
            "cap": None,
            "frame_index": 0,
            "ultima_tentativa": 0.0
        }

    try:
        while True:
            alguma_conectada = False

            for source_id, fonte in fontes.items():
                cap = fonte[
                    "cap"
                ]

                # --------------------------------------------
                # Reconexão automática
                # --------------------------------------------

                if (
                    cap is None
                    or not cap.isOpened()
                ):
                    agora = time.monotonic()

                    if (
                        agora
                        - fonte["ultima_tentativa"]
                        < TEST_RECONNECT_SECONDS
                    ):
                        continue

                    fonte[
                        "ultima_tentativa"
                    ] = agora

                    cap = conectar_camera_teste(
                        fonte["config"]
                    )

                    fonte[
                        "cap"
                    ] = cap

                    if cap is None:
                        continue

                alguma_conectada = True

                # --------------------------------------------
                # Leitura do frame
                # --------------------------------------------

                ok, frame = cap.read()

                if (
                    not ok
                    or frame is None
                ):
                    print(
                        f"[TESTE] stream perdido: "
                        f"{source_id}"
                    )

                    cap.release()

                    fonte[
                        "cap"
                    ] = None

                    continue

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

                metadados = {
                    "captured_at":
                        agora_iso_utc(),

                    "source_id":
                        source_id,

                    "session_id":
                        session_id,

                    "frame_index":
                        fonte[
                            "frame_index"
                        ],

                    "image_width":
                        largura,

                    "image_height":
                        altura,

                    "pixel_format":
                        "BGR",

                    "fps":
                        float(fps)
                }

                fonte[
                    "frame_index"
                ] += 1

                processar_frame(
                    frame,
                    metadados
                )

            if not alguma_conectada:
                time.sleep(
                    0.1
                )

    finally:
        for fonte in fontes.values():
            cap = fonte.get(
                "cap"
            )

            if cap is not None:
                cap.release()


# ============================================================
# 10. MAIN
# ============================================================

def validar_configuracao():
    if not KAFKA_BOOTSTRAP_SERVERS.strip():
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS "
            "não pode ser vazio."
        )

    if not KAFKA_TOPIC.strip():
        raise ValueError(
            "KAFKA_TOPIC "
            "não pode ser vazio."
        )

    if REPEAT_COOLDOWN_SECONDS < 1.0:
        raise ValueError(
            "I6_REPEAT_COOLDOWN_SECONDS "
            "deve ser >= 1.0."
        )

    if ACQUIRE_CONFIRM_FRAMES < 1:
        raise ValueError(
            "I6_ACQUIRE_CONFIRM_FRAMES "
            "deve ser >= 1."
        )

    if SWITCH_CONFIRM_FRAMES < 1:
        raise ValueError(
            "I6_SWITCH_CONFIRM_FRAMES "
            "deve ser >= 1."
        )

    if SWITCH_AREA_RATIO <= 0:
        raise ValueError(
            "I6_SWITCH_AREA_RATIO "
            "deve ser > 0."
        )


def main():
    validar_configuracao()

    print(
        "=" * 60
    )

    print(
        "I6 - Detector de códigos gráficos"
    )

    print(
        f"I6_TEST_MODE={MODO_TESTE}"
    )

    print(
        "=" * 60
    )

    iniciar_kafka()

    try:
        # ====================================================
        # AQUI ESTÁ A DIVISÃO PRINCIPAL PEDIDA:
        # ====================================================

        if MODO_TESTE:
            executar_modo_teste()

        else:
            executar_modo_p4()

    except KeyboardInterrupt:
        print(
            "\n[I6] encerramento solicitado."
        )

    finally:
        if producer is not None:
            producer.flush(
                10
            )

        print(
            "[I6] encerrado."
        )


if __name__ == "__main__":
    main()
