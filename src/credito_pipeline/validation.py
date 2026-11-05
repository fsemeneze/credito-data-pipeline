"""Camada de validação: contrato de dados com pandera.

O notebook original corrigia manualmente tipos errados, valores ``na`` e
categorias fora do previsto. Aqui isso vira um contrato formal: o DataFrame
precisa seguir o schema declarado ou o pipeline interrompe antes de
publicar dados ruins. Implementado na fase 1.
"""

from __future__ import annotations

from credito_pipeline.config import PipelineConfig, load_config


def validar(config: PipelineConfig | None = None) -> None:
    """Valida um DataFrame contra o contrato de dados e falha em caso de violação."""
    config = config or load_config()
    raise NotImplementedError("Implementado na fase 1")
