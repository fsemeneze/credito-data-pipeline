"""Camada de ingestão: lê os dados que chegaram na zona raw.

No fluxo normal, o gerador sintético já gravou o 'raw' particionado por
mês e a ingestão apenas o lê. Sem raw (ex.: primeiro dia do ambiente),
cai para o CSV real de calibração, atribuindo um mês de referência
corrente ao snapshot — a fonte real também precisa passar pelo contrato.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from credito_pipeline.config import PipelineConfig, load_config
from credito_pipeline.log import get_logger

logger = get_logger("ingestion")


def _ler_csv_real(paths) -> pd.DataFrame:
    """Dataset real em um painel de um snapshot só (mês corrente)."""
    df = pd.read_csv(paths.seed_dataset, na_values="na")
    df["ref_month"] = str(pd.Period(pd.Timestamp.today(), freq="M"))
    return df


def ingestar(config: PipelineConfig | None = None, limit: int | None = None) -> pd.DataFrame:
    """Lê a zona raw e devolve o DataFrame de entrada do resto do pipeline.

    Args:
        config: configuração do pipeline (padrão: ``load_config()``).
        limit: máximo de meses (partições) a ler, do mais antigo para o
            mais recente. ``None`` lê tudo.
    """
    cfg = config or load_config()
    arquivos: list[Path] = sorted(cfg.paths.raw.glob("ref_month=*/clientes.parquet"))
    if arquivos:
        if limit is not None:
            arquivos = arquivos[:limit]
        df = pd.concat((pd.read_parquet(arquivo) for arquivo in arquivos), ignore_index=True)
        logger.info("Ingestão: %d partições de raw, %d linhas", len(arquivos), len(df))
        return df

    logger.info("Raw vazia: ingerindo o CSV real de calibração como fallback")
    return _ler_csv_real(cfg.paths)
