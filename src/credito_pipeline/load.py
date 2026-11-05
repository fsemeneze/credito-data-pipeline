"""Camada de carga: gravação dos resultados e switches de storage.

Grava Parquet particionado por mês de referência, com switch de ambiente
``local``/``s3`` (fase 2) sem alterar os módulos de transformação.
Implementado na fase 1 (storage local) e fase 2 (S3/MinIO).
"""

from __future__ import annotations

from credito_pipeline.config import PipelineConfig, load_config


def carregar(config: PipelineConfig | None = None) -> None:
    """Grava os resultados processados na camada configurada."""
    config = config or load_config()
    raise NotImplementedError("Implementado na fase 1")
