"""CLI do pipeline.

Uso na fase 0 (os comandos ainda não estão implementados)::

    credito --version
    credito run --limit 1000    # executa o pipeline com amostra pequena
"""

from __future__ import annotations

import argparse
import sys

import credito_pipeline
from credito_pipeline import config as _config
from credito_pipeline import transformation, validation
from credito_pipeline.generation import gerar_clientes
from credito_pipeline.ingestion import ingestar
from credito_pipeline.load import carregar
from credito_pipeline.log import get_logger, setup_logging

logger = get_logger("cli")


def _executar_pipeline(limit: int | None) -> int:
    """Executa o pipeline de ponta a ponta (gera -> valida -> transforma -> carrega)."""
    cfg = _config.load_config()
    if limit is not None:
        logger.info("Modo amostra: limit=%d", limit)
    try:
        gerar_clientes(cfg)
        ingestar(cfg)
        validation.validar(cfg)
        transformation.transformar(cfg)
        carregar(cfg)
    except NotImplementedError as exc:
        logger.error("Etapa não implementada: %s", exc)
        return 2
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
