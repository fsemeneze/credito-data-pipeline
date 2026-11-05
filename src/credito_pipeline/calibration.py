"""Perfil de calibração: utilitários sobre o dataset real.

O CSV real vem com a mesma "vida" que o notebook original enfrentava:
valores ``na``, moeda em formato brasileiro (``12.691,51``) e sem divisão
entre o dado bruto e o dado utilizável. Este módulo deixa essas
conversões num único lugar, para o gerador sintético e as próximas
camadas consumirem o dado na mesma régua.
"""

from __future__ import annotations

import pandas as pd

from credito_pipeline.config import Paths
from credito_pipeline.log import get_logger

logger = get_logger("calibration")

COLS_MONETARIO = ("limite_credito", "valor_transacoes_12m")
COLS_CATEGORICO = (
    "sexo",
    "escolaridade",
    "estado_civil",
    "salario_anual",
    "tipo_cartao",
)


def parse_moeda_br(valor: object) -> float:
    """Converte a moeda no formato brasileiro (``12.691,51``) para float.

    Valores ausentes ou não interpretáveis viram NaN, preservando o
    comportamento de limpeza que o notebook original precisava fazer.
    """
    if pd.isna(valor):
        return float("nan")
    try:
        texto = str(valor).strip().replace(".", "").replace(",", ".")
        return float(texto)
    except ValueError:
        return float("nan")


def carregar_dataset_real(paths: Paths) -> pd.DataFrame:
    """Lê o dataset de calibração do jeito que a fonte entrega."""
    df = pd.read_csv(paths.seed_dataset, na_values="na")
    logger.info(
        "Dataset real carregado: %d linhas, taxa de inadimplência=%.3f",
        len(df),
        df["default"].mean(),
    )
    return df


def dataset_para_geracao(paths: Paths) -> pd.DataFrame:
    """Dataset real com moeda convertida em float e imputações mínimas.

    O gerador trabalha com atributos numéricos coerentes: a moeda vira
    float e o que não existe no real (ausências pontuais) é preenchido
    com a mediana — então a 'sujeira' de produção entra, downstream,
    só pelo gerador (delta intencional em ``aplicar_sujeira``).
    """
    df = carregar_dataset_real(paths)
    for col in COLS_MONETARIO:
        df[col] = df[col].map(parse_moeda_br)

    df = df.dropna(subset=COLS_CATEGORICO).reset_index(drop=True)

    for col in COLS_MONETARIO:
        df[col] = df[col].fillna(df[col].median())

    return df
