"""Geração de dados sintéticos calibrados no dataset real.

Cria ~500 mil clientes com snapshots mensais (24 meses), preservando as
distribuições e correlações do dataset de calibração (``data/creditos_clientes.csv``),
especialmente a relação de ``default`` com salário, movimentação financeira e
escolaridade. Implementado na fase 1.
"""

from __future__ import annotations

from credito_pipeline.config import PipelineConfig, load_config


def gerar_clientes(config: PipelineConfig | None = None) -> None:
    """Gera o dataset sintético e grava o parquet ``raw`` particionado por mês de referência."""
    config = config or load_config()
    raise NotImplementedError("Implementado na fase 1")
