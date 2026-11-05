"""Testes de fumaça da fundação do projeto (fase 0).

Garantem que o pacote, a CLI e os módulos carregam sem quebrar — a base
que o CI deverá proteger enquanto as fases 1 e 2 vão chegando.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import credito_pipeline
from credito_pipeline.config import load_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_versao_definida():
    assert credito_pipeline.__version__


def test_config_carrega():
    config = load_config()
    assert config.generation.n_clientes > 0
    assert config.paths.seed_dataset.name == "creditos_clientes.csv"


def test_modulos_importaveis():
    import credito_pipeline.cli  # noqa: F401
    import credito_pipeline.generation  # noqa: F401
    import credito_pipeline.ingestion  # noqa: F401
    import credito_pipeline.load  # noqa: F401
    import credito_pipeline.transformation  # noqa: F401
    import credito_pipeline.validation  # noqa: F401


def test_cli_reporta_versao():
    resultado = subprocess.run(
        [sys.executable, "-m", "credito_pipeline.cli", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert credito_pipeline.__version__ in resultado.stdout


def test_cli_run_bloqueado_ate_fase_1(tmp_path, monkeypatch):
    """Com comandos do pipeline ainda não implementados: saída controlada, sem traceback."""
    import credito_pipeline.cli as cli_mod  # noqa: F401 — garante import da CLI antes do subprocess
    from credito_pipeline.config import GenerationConfig, Paths, PipelineConfig

    paths = Paths(
        root=PROJECT_ROOT,
        data=PROJECT_ROOT / "data",
        raw=tmp_path / "raw",
        trusted=tmp_path / "trusted",
        curated=tmp_path / "curated",
        metrics=tmp_path / "metrics",
    )
    gen = GenerationConfig(n_clientes=10, meses_historico=2, mes_final="2026-09", seed=1)
    monkeypatch.setattr(
        "credito_pipeline.config.load_config", lambda: PipelineConfig(paths=paths, generation=gen)
    )

    # Roda a funcao de execucao diretamente (monkeypatch so vale neste processo).
    rc = cli_mod._executar_pipeline(limit=10)
    assert rc == 0
    assert len(list(paths.trusted.glob("ref_month=*/dados.parquet"))) == 2
    assert len(list(paths.curated.glob("ref_month=*/dados.parquet"))) == 2
