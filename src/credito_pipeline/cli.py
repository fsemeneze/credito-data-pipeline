"""CLI do pipeline.

Uso::

    credito --version
    credito run --limit 1000    # amostra de 1000 clientes, roda em segundos
    credito run                 # volume completo (500 mil clientes x 24 meses)
"""

from __future__ import annotations

import argparse
import sys

import credito_pipeline
from credito_pipeline import config as _config
from credito_pipeline import load, transformation, validation
from credito_pipeline.generation import gerar_clientes
from credito_pipeline.ingestion import ingestar
from credito_pipeline.log import get_logger, setup_logging

logger = get_logger("cli")


def _executar_pipeline(limit: int | None) -> int:
    """Executa o pipeline de ponta a ponta: gerar, ingerir, validar,
    transformar e carregar."""
    cfg = _config.load_config()
    if limit is not None:
        logger.info("Modo amostra: limit=%d clientes", limit)
    gerar_clientes(cfg, limit=limit)
    raw = ingestar(cfg)
    coerced = transformation.coercir(raw)
    trusted, relatorio = validation.validar(coerced)
    curated = transformation.enriquecer(trusted)
    load.carregar(trusted, cfg, camada="trusted", relatorio=relatorio.to_dict())
    load.carregar(curated, cfg, camada="curated")
    logger.info("Pipeline concluído com sucesso")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada do CLI."""
    parser = argparse.ArgumentParser(prog="credito", description=__doc__)
    parser.add_argument("--version", action="version", version=credito_pipeline.__version__)
    sub = parser.add_subparsers(dest="comando", required=True)

    run = sub.add_parser("run", help="Executa o pipeline de ponta a ponta")
    run.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Número de clientes para executar em modo amostra (ex.: 1000)",
    )

    args = parser.parse_args(argv)
    setup_logging()

    if args.comando == "run":
        return _executar_pipeline(args.limit)
    return 1


if __name__ == "__main__":
    sys.exit(main())
