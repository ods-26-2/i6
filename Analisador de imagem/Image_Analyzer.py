import os

import cv2
import numpy as np


# LIMITES POSSÍVEIS DAS MEDIDAS
# brightness_mean: 0 a 255
# brightness_std: 0 a 127.5
# blur_score: mínimo 0, sem máximo fixo; maior geralmente indica mais nitidez
# noise_score: mínimo 0, sem máximo fixo; maior indica mais variações de alta frequência
# dark_pixels_percentage / bright_pixels_percentage: 0 a 100%
# pixel_distribution: quantidade de pixels por nível de cinza (0 a 255)
# pixel_distribution_percentage: percentual de pixels por nível de cinza


def analisar_frame(imagem):
    """Calcula métricas de qualidade de um frame OpenCV em formato BGR."""
    if imagem is None:
        raise ValueError("A imagem recebida é inválida.")

    if not isinstance(imagem, np.ndarray) or imagem.size == 0:
        raise ValueError("A imagem deve ser um array NumPy não vazio.")

    if imagem.ndim == 2:
        cinza = imagem
    elif imagem.ndim == 3 and imagem.shape[2] == 3:
        cinza = cv2.cvtColor(imagem, cv2.COLOR_BGR2GRAY)
    else:
        raise ValueError("Formato de imagem não suportado. Esperado BGR ou escala de cinza.")

    brilho_medio = float(np.mean(cinza))
    desvio_brilho = float(np.std(cinza))

    laplaciano = cv2.Laplacian(cinza, cv2.CV_64F)
    blur_score = float(laplaciano.var())
    noise_score = float(np.median(np.abs(laplaciano)))

    histograma = cv2.calcHist([cinza], [0], None, [256], [0, 256])
    distribuicao_pixels = histograma.flatten().astype(int).tolist()

    total_pixels = int(cinza.size)
    distribuicao_percentual = [
        round((quantidade / total_pixels) * 100, 4)
        for quantidade in distribuicao_pixels
    ]

    if brilho_medio < 70:
        illumination = "underexposed"
    elif brilho_medio > 200:
        illumination = "overexposed"
    else:
        illumination = "adequate"

    percentual_escuro = float(np.sum(cinza < 30) / total_pixels * 100)
    percentual_claro = float(np.sum(cinza > 225) / total_pixels * 100)

    return {
        "blur_score": blur_score,
        "noise_score": noise_score,
        "illumination": illumination,
        "brightness_mean": brilho_medio,
        "brightness_std": desvio_brilho,
        "dark_pixels_percentage": percentual_escuro,
        "bright_pixels_percentage": percentual_claro,
        "pixel_distribution": distribuicao_pixels,
        "pixel_distribution_percentage": distribuicao_percentual,
    }


if __name__ == "__main__":
    pasta_programa = os.path.dirname(os.path.abspath(__file__))
    caminho_imagem = os.path.join(pasta_programa, "imagemdeteste.jpg")
    imagem = cv2.imread(caminho_imagem)

    if imagem is None:
        raise SystemExit(f"Não foi possível carregar a imagem: {caminho_imagem}")

    resultado = analisar_frame(imagem)
    print("\n===== ANÁLISE DA IMAGEM =====")
    for medida, valor in resultado.items():
        if medida not in ("pixel_distribution", "pixel_distribution_percentage"):
            print(f"{medida}: {valor}")

    print("\n===== DISTRIBUIÇÃO DOS PIXELS =====")
    for nivel, quantidade in enumerate(resultado["pixel_distribution"]):
        percentual = resultado["pixel_distribution_percentage"][nivel]
        if quantidade > 0:
            print(f"Nível {nivel:3d}: {quantidade:8d} pixels ({percentual:7.4f}%)")
