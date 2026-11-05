"""Logging estruturado padrão do projeto."""

from __future__ import annotations

import logging

FORMATO = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Configura o logging raiz com um formato consistente para todos os módulos."""
    logging.basicConfig(level=level, format=FORMATO, datefmt="%Y-%m-%d %H:%M:%S")
    # Ruído de bibliotecas de terceiros, limita o nível de verbosidade básico.
    logging.getLogger("matplotlib").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Logger com namespace padronizado ``credito_pipeline.<modulo>``."""
    return logging.getLogger(f"credito_pipeline.{name}")
