"""Testes de fumaça da fundação do projeto (fase 0).

Garantem que o pacote, a CLI e os módulos carregam sem quebrar — a base
que o CI deverá proteger enquanto as fases 1 e 2 vão chegando.
"""

from __future__ import annotations

import subprocess
import sys

import credito_pipeline
from credito_pipeline.config import load_config


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


def test_cli_run_bloqueado_até_fase_1():
    # Comandos do pipeline ainda não implementados: saída controlada, sem traceback.
    resultado = subprocess.run(
        [sys.executable, "-m", "credito_pipeline.cli", "run", "--limit", "10"],
        capture_output=True,
        text=True,
    )
    assert resultado.returncode == 2
    assert "Implementado na fase 1" in (resultado.stderr + resultado.stdout)
