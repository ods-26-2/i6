"""Testes do leitor I6. Não conectam ao Kafka nem ao P4 real."""
import json
import struct
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

import cv2
import numpy as np
import leitor as i6


SESSION = str(uuid.UUID(int=1))
UTC = '2026-01-01T12:00:00.000Z'


def pacote(formato='BGR8', **mudancas):
    """Monta um pacote sintético no protocolo P4R1 do P4."""
    if formato == 'RG10':
        pixels = np.full((8, 8), 512, dtype='<u2').tobytes()
    else:
        pixels = np.full((8, 8, 3), (10, 20, 30), dtype=np.uint8).tobytes()
    campos = dict(source_id='imx519' if formato == 'RG10' else 'file_teste',
                  session_id=SESSION, frame_id=7, width=8, height=8,
                  format=formato, raw=formato == 'RG10', data_size=len(pixels),
                  timestamp=None)
    campos.update(mudancas)
    header = json.dumps(campos).encode('utf-8')
    return b'P4R1' + struct.pack('!I', len(header)) + header + pixels


def leitura(texto='QR-TESTE', area=100):
    return dict(code_type='QR_CODE', decoded_data=texto, bbox=[0, 0, 10, 10], area=area)


class TestP4(unittest.TestCase):
    @patch.object(i6, 'agora_iso_utc', return_value=UTC)
    @patch.object(i6, 'P4_EXIGIR_TIMESTAMP', False)
    def test_video_bgr8(self, _):
        frame, meta = i6.decodificar_pacote_p4r1(pacote())
        self.assertEqual(frame.shape, (8, 8, 3))
        self.assertEqual(frame[0, 0].tolist(), [10, 20, 30])
        self.assertEqual((meta['source_id'], meta['session_id'], meta['frame_index']),
                         ('file_teste', SESSION, 7))
        self.assertEqual(meta['captured_at'], UTC)

    @patch.object(i6, 'P4_EXIGIR_TIMESTAMP', False)
    def test_camera_rg10(self):
        frame, meta = i6.decodificar_pacote_p4r1(pacote('RG10'))
        self.assertEqual(frame.dtype, np.uint8)
        self.assertEqual(frame.shape, (8, 8, 3))
        self.assertTrue(np.all(frame == 128))
        self.assertEqual(meta['source_id'], 'imx519')

    def test_pacotes_invalidos(self):
        for dados in [b'BAD!', pacote()[:-1], pacote(width=0), pacote(frame_id=-1),
                      pacote(session_id='invalido'), pacote(data_size=1),
                      pacote(format='XYZ'), b'P4R1'+struct.pack('!I', 999999)]:
            with self.subTest(dados=dados[:8]), self.assertRaises(ValueError):
                i6.decodificar_pacote_p4r1(dados)

    @patch.object(i6, 'raw_ns_para_utc', return_value=UTC)
    def test_timestamp_camera_normalizado(self, converter):
        _, meta = i6.decodificar_pacote_p4r1(pacote('RG10',
            timestamp_monotonic_raw_ns=123, timestamp_monotonic_raw_clock='CLOCK_MONOTONIC_RAW'))
        converter.assert_called_once_with(123)
        self.assertEqual(meta['captured_at'], UTC)

    @patch.object(i6, 'raw_ns_para_utc', return_value=UTC)
    def test_timestamp_reproducao_video(self, converter):
        _, meta = i6.decodificar_pacote_p4r1(pacote(
            received_at_monotonic_raw_ns=456, received_at_monotonic_raw_clock='CLOCK_MONOTONIC_RAW'))
        converter.assert_called_once_with(456)
        self.assertEqual(meta['captured_at'], UTC)

    @patch.object(i6, 'P4_EXIGIR_TIMESTAMP', True)
    def test_timestamp_obrigatorio(self):
        with self.assertRaises(ValueError):
            i6.decodificar_pacote_p4r1(pacote('RG10', timestamp=123))

    def test_selecao_fontes(self):
        for modo, esperado in [('camera', ['imx519', 'imx477']), ('video', ['video']),
                                ('ambos', ['imx519', 'imx477', 'video'])]:
            with self.subTest(modo=modo), patch.multiple(i6, MODO_FONTE=modo, P4_CAMERAS='imx519,imx477'):
                self.assertEqual(list(i6.selecionar_sockets_p4()), esperado)

    def test_camera_invalida(self):
        with patch.multiple(i6, MODO_FONTE='camera', P4_CAMERAS='desconhecida'):
            with self.assertRaises(ValueError):
                i6.selecionar_sockets_p4()


class TestDeteccao(unittest.TestCase):
    def test_qr_real_gerado_em_memoria(self):
        qr = cv2.QRCodeEncoder_create().encode('QR-TESTE-I6')
        qr = cv2.copyMakeBorder(qr, 4, 4, 4, 4, cv2.BORDER_CONSTANT, value=255)
        qr = cv2.resize(qr, None, fx=8, fy=8, interpolation=cv2.INTER_NEAREST)
        resultados = i6.detectar_e_decodificar(cv2.cvtColor(qr, cv2.COLOR_GRAY2BGR))
        self.assertIn('QR-TESTE-I6', [r['decoded_data'] for r in resultados])

    @patch.object(i6, 'decode')
    def test_qr_binario_aes_hex(self, decode):
        dados = bytes.fromhex('ff0102030405060708090a0b0c0d0e0f')
        decode.return_value = [SimpleNamespace(type='QRCODE', data=dados, rect=(1, 2, 5, 6))]
        resultado = i6.detectar_e_decodificar(np.zeros((20, 20, 3), np.uint8))[0]
        self.assertEqual(resultado['decoded_data'], dados.hex())
        self.assertEqual(resultado['bbox'], [1, 2, 6, 8])

    @patch.object(i6, 'decode')
    def test_tipo_nao_suportado(self, decode):
        decode.return_value = [SimpleNamespace(type='PDF417', data=b'teste')]
        self.assertEqual(i6.detectar_e_decodificar(np.zeros((20, 20, 3), np.uint8)), [])


class TestEstadoKafka(unittest.TestCase):
    def setUp(self):
        i6.estados.clear()
        i6.sequencia_por_sessao.clear()
        p = patch.multiple(i6, ACQUIRE_CONFIRM_FRAMES=4, SWITCH_CONFIRM_FRAMES=3,
                           SWITCH_AREA_RATIO=1.5, REPEAT_COOLDOWN_SECONDS=3, QR_LOST_SECONDS=3)
        p.start()
        self.addCleanup(p.stop)

    def escolha(self, t, n, leituras, fonte='cam0'):
        with patch.object(i6.time, 'monotonic', return_value=t):
            return i6.escolher_leitura_estavel(fonte, SESSION, n, UTC, leituras)

    def test_confirmacao_e_cooldown(self):
        qr = leitura()
        for n in range(3):
            self.assertIsNone(self.escolha(10, n, [qr]))
        self.assertEqual(self.escolha(10, 3, [qr]), qr)
        self.assertIsNone(self.escolha(12.9, 4, [qr]))
        self.assertEqual(self.escolha(13, 5, [qr]), qr)

    def test_troca_de_qr(self):
        antigo, pequeno, grande = leitura(), leitura('B', 120), leitura('C', 200)
        for n in range(4):
            self.escolha(10, n, [antigo])
        self.assertIsNone(self.escolha(11, 4, [pequeno]))
        for n in (5, 6):
            self.assertIsNone(self.escolha(12, n, [grande]))
        self.assertEqual(self.escolha(12, 7, [grande]), grande)

    def test_estado_independente_e_nova_sessao(self):
        a = i6.obter_estado('cam0', SESSION)
        self.assertTrue(i6.validar_ordem_frame(a, 10))
        self.assertFalse(i6.validar_ordem_frame(a, 10))
        self.assertFalse(i6.validar_ordem_frame(a, 9))
        self.assertTrue(i6.validar_ordem_frame(i6.obter_estado('cam1', SESSION), 0))
        self.assertTrue(i6.validar_ordem_frame(i6.obter_estado('cam0', str(uuid.UUID(int=2))), 0))

    def test_evento_e_sequencia(self):
        eventos = [i6.montar_evento_i6('cam0', SESSION, UTC, n, leitura()) for n in (1, 2)]
        for evento in eventos:
            i6.validar_evento_i6(evento)
            self.assertEqual(evento['schema'], 'ods.i6.code_read')
            self.assertEqual(evento['captured_at'], UTC)
        self.assertEqual([e['sequence'] for e in eventos], [0, 1])
        self.assertNotEqual(eventos[0]['message_id'], eventos[1]['message_id'])

    def test_evento_fora_do_contrato(self):
        evento = i6.montar_evento_i6('cam0', SESSION, UTC, 1, leitura())
        evento['payload']['bbox'] = [1, 2]
        with self.assertRaises(ValueError):
            i6.validar_evento_i6(evento)

    @patch.object(i6, 'producer')
    @patch.object(i6, 'KAFKA_TOPIC', 'ods.i6.code_read')
    def test_publicacao_kafka(self, producer):
        i6.publicar_leitura('cam0', SESSION, UTC, 7, leitura())
        kwargs = producer.produce.call_args.kwargs
        self.assertEqual(kwargs['topic'], 'ods.i6.code_read')
        self.assertEqual(kwargs['key'], b'cam0')
        self.assertEqual(json.loads(kwargs['value'])['payload']['frame_index'], 7)
        producer.poll.assert_called_once_with(0)

    @patch.object(i6, 'publicar_leitura')
    @patch.object(i6, 'detectar_e_decodificar', return_value=[leitura()])
    def test_frame_duplicado_nao_processado(self, detectar, publicar):
        meta = dict(source_id='cam0', session_id=SESSION, frame_index=1, captured_at=UTC)
        frame = np.zeros((10, 10, 3), np.uint8)
        i6.processar_frame(frame, meta)
        i6.processar_frame(frame, meta)
        detectar.assert_called_once()
        publicar.assert_not_called()


if __name__ == '__main__':
    unittest.main()
