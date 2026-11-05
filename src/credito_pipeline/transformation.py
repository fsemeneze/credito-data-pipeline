"""Camada de transformação: correção, deduplicação e features analíticas.

Recebe os dados validados da camada ``trusted`` e produz a camada ``curated``,
com dados consistentes e prontos para consumo (análise e BI).
Implementado na fase 1.
"""

from __future__ import annotations

from credito_pipeline.config import PipelineConfig, load_config


def transformar(config: PipelineConfig | None = None) -> None:
    """Aplica transformações e grava a camada ``curated``."""
    config = config or load_config()
    raise NotImplementedError("Implementado na fase 1")
