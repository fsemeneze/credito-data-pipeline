"""Configuração central do pipeline.

Centraliza camadas de dados, caminhos e seleção de ambiente para que
nenhum módulo precise adivinhar onde ler ou gravar arquivos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Paths:
    """Caminhos das camadas de dados (layout medallion simplificado)."""

    root: Path = PROJECT_ROOT
    data: Path = field(default=PROJECT_ROOT / "data")
    raw: Path = field(default=PROJECT_ROOT / "data" / "raw")
    trusted: Path = field(default=PROJECT_ROOT / "data" / "trusted")
    curated: Path = field(default=PROJECT_ROOT / "data" / "curated")

    @property
    def seed_dataset(self) -> Path:
        """Dataset real de calibração (contrato de referência)."""
        return self.data / "creditos_clientes.csv"


@dataclass(frozen=True)
class GenerationConfig:
    """Parâmetros do gerador de dados sintéticos (fase 1)."""

    n_clientes: int = 500_000
    meses_historico: int = 24
    mes_final: str = "2026-09"
    seed: int = 42


@dataclass(frozen=True)
class PipelineConfig:
    """Configuração agregada usada em toda a aplicação."""

    paths: Paths = field(default_factory=Paths)
    generation: GenerationConfig = field(default_factory=GenerationConfig)


def load_config() -> PipelineConfig:
    """Retorna a configuração do ambiente atual.

    Na fase 2 este ponto passa a considerar variáveis de ambiente
    (ex.: ``DATA_ENV=local|s3``) para trocar storage sem mexer no código.
    """
    return PipelineConfig()
