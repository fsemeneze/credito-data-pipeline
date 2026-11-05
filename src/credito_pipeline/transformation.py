"""Camada de transformação: coerção do raw e enriquecimento do trusted.

Duas funções intencionamente separadas:

* ``coercir`` normaliza o dado como ele chega (moeda em texto BR, caixa
  alta em categorias, números apontados como texto) para os tipos de
  contrato. Nenhuma linha é descartada aqui — isso é trabalho da
  validação, que contabiliza cada rejeição no relatório de qualidade.
* ``enriquecer`` parte do trusted (já validado) e adiciona features
  analíticas cumulativas por cliente que a análise final usa.
"""

from __future__ import annotations

import pandas as pd

from credito_pipeline.calibration import COLS_MONETARIO, parse_moeda_br
from credito_pipeline.log import get_logger
from credito_pipeline.validation import COLS_CONTAGEM

logger = get_logger("transformation")

COLS_CATEGORICAS_TEXTO = ("sexo", "escolaridade", "estado_civil", "salario_anual", "tipo_cartao")


def coercir(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza o texto/ordem do raw para os tipos de contrato.

    * moeda com formato brasileiro (``12.691,51``) vira float;
    * números chegados como texto viram numéricos (NaN se ilegíveis);
    * categorias perdem espaços e caixa alta (``BLUE`` -> ``blue``).
    """
    df = df.copy()
    for coluna in COLS_MONETARIO:
        df[coluna] = df[coluna].map(parse_moeda_br)

    for coluna in COLS_CATEGORICAS_TEXTO:
        df[coluna] = df[coluna].str.strip().str.lower()

    for coluna in COLS_CONTAGEM:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").astype("Int64")

    logger.info("Coerção aplicada")
    return df


def enriquecer(trusted: pd.DataFrame) -> pd.DataFrame:
    """Features cumulativas por cliente sobre o trusted (curated)."""
    df = trusted.copy()
    df = df.sort_values(["id", "ref_month"])

    grupos = df.groupby("id", sort=False)
    df["flag_default_historico"] = grupos["default"].cummax().astype("int64")
    df["valor_total_acumulado"] = grupos["valor_transacoes_12m"].cumsum().round(2)

    logger.info("Enriquecimento: %d linhas, %d features adicionadas", len(df), 2)
    return df.reset_index(drop=True)
