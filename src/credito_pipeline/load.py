"""Camada de carga: gravação das camadas trusted/curated e das métricas de qualidade.

O dado sai particionado por mês de referência (``ref_month=YYYY-MM``),
mesma régua do raw, e o relatório de validação acompanha a execução em
``data/metrics/`` — histórico de qualidade que a orquestração (fase 2)
consultará.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from credito_pipeline.config import PipelineConfig, load_config
from credito_pipeline.log import get_logger

logger = get_logger("load")


def _gravar_particao(df: pd.DataFrame, diretorio, camada: str, mes: str) -> None:
    particao = diretorio / f"ref_month={mes}"
    particao.mkdir(parents=True, exist_ok=True)
    arquivo = particao / "dados.parquet"
    df.to_parquet(arquivo, index=False)
    logger.info("Carga %s: %s (%d linhas)", camada, arquivo, len(df))


def carregar(
    df: pd.DataFrame,
    config: PipelineConfig | None = None,
    *,
    camada: str = "trusted",
    relatorio: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Grava o DataFrame particionado por mês e, se informado, o relatório de validação.

    Args:
        df: dados a gravar (com coluna ``ref_month`` à direita).
        config: configuração do pipeline (padrão: ``load_config()``).
        camada: ``"trusted"`` ou ``"curated"`` (caminho de ``Paths``).
        relatorio: dicionário do ``RelatorioValidacao.to_dict()``.
    """
    cfg = config or load_config()
    diretorio = getattr(cfg.paths, camada)
    if "ref_month" not in df.columns:
        raise ValueError("carga exige a coluna ref_month para particionar")

    resumo = {"camada": camada, "linhas": len(df), "meses": 0}
    for mes, parte in df.groupby("ref_month"):
        _gravar_particao(parte, diretorio, camada, str(mes))
        resumo["meses"] += 1

    logger.info(
        "Carga concluída na camada %s: %d linhas em %d partições",
        camada,
        resumo["linhas"],
        resumo["meses"],
    )

    if relatorio is not None:
        registro = {
            "gerado_em": datetime.now(UTC).isoformat(timespec="seconds"),
            "pipeline": relatorio,
        }
        cfg.paths.metrics.mkdir(parents=True, exist_ok=True)
        arquivo_metrica = cfg.paths.metrics / "validacao_trusted.json"
        arquivo_metrica.write_text(
            json.dumps(registro, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("Relatório de qualidade gravado em %s", arquivo_metrica)

    return resumo
