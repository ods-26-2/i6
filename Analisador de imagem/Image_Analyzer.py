import cv2
import numpy as np
import os


# LIMITES POSSÍVEIS DAS MEDIDAS

# brilho_medio:
# Mínimo: 0
# Máximo: 255

# desvio_brilho:
# Mínimo: 0
# Máximo: 127.5

# blur_score:
# Mínimo: 0
# Máximo: não possui limite fixo
# Quanto maior o blur_score, mais nítida é a imagem.

# noise_score:
# Mínimo: 0
# Máximo: não possui limite fixo
# Quanto maior o noise_score, mais ruído é detectado na imagem.

# dark_pixels_percentage:
# Mínimo: 0%
# Máximo: 100%

# bright_pixels_percentage:
# Mínimo: 0%
# Máximo: 100%

# pixel_distribution:
# Mínimo por nível: 0 pixels
# Máximo por nível: total de pixels da imagem

# pixel_distribution_percentage:
# Mínimo: 0%
# Máximo: 100%


def analisar_frame(imagem):

    if imagem is None:
        raise ValueError(
            "A imagem recebida é inválida."
        )

    cinza = cv2.cvtColor(
        imagem,
        cv2.COLOR_BGR2GRAY
    )

    # 1. BRILHO MÉDIO
    brilho_medio = float(np.mean(cinza))

    # 2. DESVIO PADRÃO DO BRILHO
    desvio_brilho = float(np.std(cinza))

    # 3. ANÁLISE DE DESFOQUE
    blur_score = float(
        cv2.Laplacian(
            cinza,
            cv2.CV_64F
        ).var()
    )

    # 4. ANÁLISE DE RUÍDO
    # O ruído é estimado a partir do Laplaciano da imagem. Quanto maior o noise_score, maior a quantidade de variações de alta frequência detectadas na imagem.
    laplaciano = cv2.Laplacian(
        cinza,
        cv2.CV_64F
    )
    noise_score = float(
        np.median(
            np.abs(laplaciano)
        )
    )

    # 5. HISTOGRAMA / DISTRIBUIÇÃO DOS PIXELS
    histograma = cv2.calcHist(
        [cinza],
        [0],
        None,
        [256],
        [0, 256]
    )

    # Quantidade de pixels em cada nível de cinza
    distribuicao_pixels = (
        histograma
        .flatten()
        .astype(int)
        .tolist()
    )

    # 6. DISTRIBUIÇÃO PERCENTUAL
    total_pixels = cinza.size

    distribuicao_percentual = [
        round((quantidade / total_pixels) * 100, 4)
        for quantidade in distribuicao_pixels
    ]

    # 7. CLASSIFICAÇÃO DA ILUMINAÇÃO
    if brilho_medio < 70:

        illumination = "underexposed"

    elif brilho_medio > 200:

        illumination = "overexposed"

    else:

        illumination = "adequate"

    # 8. PIXELS MUITO ESCUROS
    pixels_escuros = np.sum(cinza < 30)

    percentual_escuro = float(
        pixels_escuros /
        total_pixels *
        100
    )

    # 9. PIXELS MUITO CLAROS
    pixels_claros = np.sum(cinza > 225)

    percentual_claro = float(
        pixels_claros /
        total_pixels *
        100
    )

    return {
        "blur_score": blur_score,
        "noise_score": noise_score,
        "illumination": illumination,
        "brightness_mean": brilho_medio,
        "brightness_std": desvio_brilho,
        "dark_pixels_percentage": percentual_escuro,
        "bright_pixels_percentage": percentual_claro,
        "pixel_distribution": distribuicao_pixels,
        "pixel_distribution_percentage":
            distribuicao_percentual
    }


if __name__ == "__main__":

    pasta_programa = os.path.dirname(
        os.path.abspath(__file__)
    )

    caminho_imagem = os.path.join(
        pasta_programa,
        "imagemdeteste.jpg"
    )

    resultado = analisar_frame(caminho_imagem)

    print("\n===== ANÁLISE DA IMAGEM =====")

    for medida, valor in resultado.items():

        if medida not in (
            "pixel_distribution",
            "pixel_distribution_percentage"
        ):
            print(f"{medida}: {valor}")

    print("\n===== DISTRIBUIÇÃO DOS PIXELS =====")

    for nivel in range(256):

        quantidade = resultado["pixel_distribution"][nivel]

        percentual = resultado[
            "pixel_distribution_percentage"
        ][nivel]

        if quantidade > 0:
            print(
                f"Nível {nivel:3d}: "
                f"{quantidade:8d} pixels "
                f"({percentual:7.4f}%)"
            )
