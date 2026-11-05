"""Camada de ingestão: amostra do CSV de calibração e parquet ``raw``.

Duas entradas de dados convivem neste projeto:

* o dataset real de calibração (10.127 linhas), usado como contrato de
  referência para o gerador sintético;
* o dataset sintético em escala, gravado como Parquet particionado.

Implementado na fase 1.
"""

from __future__ import annotations

from credito_pipeline.config import PipelineConfig, load_config


def ingestar(config: PipelineConfig | None = None) -> None:
    """Testa ler a fonte e grava registros na camada ``raw`` (Parquet)."""
    config = config or load_config()
    raise NotImplementedError("Implementado na fase 1")
