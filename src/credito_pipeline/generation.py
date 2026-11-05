"""Gerador de dados sintéticos calibrado no dataset real.

Expande os ~10 mil registros reais para um painel de observações mensais
(centenas de milhares de clientes x meses de histórico). Três decisões
de projeto definem este módulo:

1. **Bootstrap com reposição** das linhas reais para formar clientes-base:
   preserva a distribuição conjunta exata dos atributos (escolaridade,
   salário, movimentação...) sem exigir modelos estatísticos paramétricos.

2. **Dimensão temporal por snapshots mensais**: evolui atributos que
   naturalmente se movem com o tempo (idade, relacionamento, transações)
   e simula a inadimplência como uma cadeia de Markov de dois estados
   (adimplente/inadimplente), calibrada para reproduzir a taxa real.

3. **Fidelidade ao formato da fonte**: a moeda é gravada como texto no
   formato brasileiro (``12.691,51``), como a fonte real guarda, e uma
   fração pequena das linhas sai com ausências (``na``) e categorias
   fora do padrão — imperfeções de produção para a camada de validação
   detectar e tratar.

O volume é gravado mês a mês em ``data/raw/ref_month=YYYY-MM/``, o que
mantém a memória contida e já prepara o particionamento que o Airflow
usará para cargas incrementais na fase 2.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from credito_pipeline.calibration import COLS_CATEGORICO, dataset_para_geracao
from credito_pipeline.config import Paths, PipelineConfig, load_config
from credito_pipeline.log import get_logger

logger = get_logger("generation")

# Probabilidade mensal de um cliente inadimplente voltar a ficar adimplente.
P_RECUPERACAO = 0.25
# Quanto maior, mais diferentes as taxas de entrada no default de clientes
# que já inadimpliram no dataset real versus os adimplentes.
FATOR_RISCO = 1.5

# Proporções mensais de cada tipo de "sujeira" intencional (delta do gerador).
PROP_VALOR_NAO_DISPONIVEL = 0.0015
PROP_CATEGORIA_FORA_PADRAO = 0.002


def _arredondar_para_int(serie: pd.Series, minimo: int, maximo: int | None) -> pd.Series:
    """Série de contagem: arredonda e recorta para o domínio permitido."""
    limites = {"lower": minimo} if maximo is None else {"lower": minimo, "upper": maximo}
    return serie.clip(**limites).round().astype(int)


def _ruido_contagem(
    base: pd.Series, escala: float, minimo: int, maximo: int | None, rng: np.random.Generator
) -> pd.Series:
    """Variação mensal de contagens ao redor do valor-base do cliente."""
    ruido = np.round(rng.normal(0.0, escala, len(base)))
    return _arredondar_para_int(base.astype(float) + ruido, minimo, maximo)


def _moeda_formato_br(valor: float) -> str:
    """Escreve moeda com separadores brasileiros, como na fonte real (``12.691,51``)."""
    return f"{valor:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def _construir_clientes(
    amostra: pd.DataFrame, n_clientes: int, colunas_base: list[str]
) -> pd.DataFrame:
    """Clientes-base a partir do bootstrap com reposição sobre o dataset real."""
    clientes = pd.DataFrame(index=range(n_clientes))
    # Ids sintéticos em um segmento distinto dos reais (evita colisão).
    clientes["id"] = 900_000_000 + np.arange(n_clientes, dtype=np.int64)
    for coluna in colunas_base:
        clientes[coluna] = amostra[coluna].to_numpy()
    # Propensão herdada da linha de origem: carrega as correlações
    # atributo-/default que vêm do dataset real.
    clientes["default_base"] = amostra["default"].to_numpy().astype(int)
    return clientes


def _calibrar_p_entrada(taxa_real: float, multiplicador_medio: float) -> tuple[float, float]:
    """Resolve a probabilidade mensal de entrada no default.

    Num processo de dois estados com recuperação fixa ``p_recuperacao``
    e entrada ``p_entrada``, a taxa estacionária de inadimplência é
    ``pi = p_entrada / (p_entrada + p_recuperacao)``. Como clientes
    com propensão herdada de inadimplência entram ``FATOR_RISCO`` vezes
    mais, o multiplicador médio entra no cálculo para que a taxa geral
    da série sintética conserve a taxa do dataset real.
    """
    p_entrada = taxa_real * P_RECUPERACAO / ((1.0 - taxa_real) * multiplicador_medio)
    return p_entrada, taxa_real


def _snapshot_mensal(
    clientes: pd.DataFrame,
    prev_default: np.ndarray,
    mes: pd.Period,
    indice_mes: int,
    p_entrada: float,
    rng: np.random.Generator,
    colunas_base: list[str],
) -> pd.DataFrame:
    """Um mês de observações para todos os clientes (linha x cliente)."""
    n_clientes = len(clientes)
    df = clientes.drop(columns=["default_base"]).copy()

    if indice_mes == 0:
        df["default"] = prev_default
    else:
        # Cadeia de Markov mensal: permanece em default com 1 - P_RECUPERACAO;
        # clientes propensos (linha de origem inadimplente) entram FATOR_RISCO
        # vezes mais, o que mantém a dependência herdada dos atributos.
        p_entrada_linha = p_entrada * np.where(
            clientes["default_base"].to_numpy() == 1, 1.0 + FATOR_RISCO, 1.0
        )
        permanece = (prev_default == 1) & (rng.random(n_clientes) >= P_RECUPERACAO)
        entra = (prev_default == 0) & (rng.random(n_clientes) < p_entrada_linha)
        df["default"] = (permanece | entra).astype(int)

    df["ref_month"] = str(mes)
    df["idade"] = clientes["idade"] + indice_mes // 12
    df["meses_de_relacionamento"] = clientes["meses_de_relacionamento"] + indice_mes

    df["qtd_produtos"] = _ruido_contagem(clientes["qtd_produtos"], 0.6, 1, 8, rng)
    df["iteracoes_12m"] = _ruido_contagem(clientes["iteracoes_12m"], 1.0, 0, 17, rng)
    df["meses_inativo_12m"] = _ruido_contagem(clientes["meses_inativo_12m"], 0.8, 0, 12, rng)
    df["qtd_transacoes_12m"] = _ruido_contagem(clientes["qtd_transacoes_12m"], 3.0, 0, None, rng)

    fator_linha = np.exp(rng.normal(0.0, 0.04, n_clientes))
    df["limite_credito"] = (clientes["limite_credito"] * fator_linha).round(2)
    fator_linha = np.exp(rng.normal(0.0, 0.10, n_clientes))
    df["valor_transacoes_12m"] = (clientes["valor_transacoes_12m"] * fator_linha).round(2)

    # Raw guarda o dado como a fonte entrega: moeda em texto BR.
    df["limite_credito"] = df["limite_credito"].map(_moeda_formato_br)
    df["valor_transacoes_12m"] = df["valor_transacoes_12m"].map(_moeda_formato_br)

    # Ordem canônica: igual ao CSV real, com ref_month por último.
    return df[["id", "default", *colunas_base, "ref_month"]]


def aplicar_sujeira(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Injeita, em proporções pequenas, imperfeções vistas na fonte real.

    Retorna um novo DataFrame; o objetivo é deixar o 'raw' fiel a uma
    captura de produção (ausências e categoria fora do padrão), que a
    camada de validação então detecta e trata.
    """
    df = df.copy()
    n = len(df)

    # Ausências pontuais em campos variados.
    for coluna in ("qtd_transacoes_12m", "valor_transacoes_12m", "escolaridade", "estado_civil"):
        mascara = rng.random(n) < PROP_VALOR_NAO_DISPONIVEL
        if mascara.any():
            df[coluna] = df[coluna].astype("object")
            df.loc[mascara, coluna] = np.nan

    # Categoria fora do padrão (só o tipo de cartão tem caixa alta no real).
    mascara = rng.random(n) < PROP_CATEGORIA_FORA_PADRAO
    df.loc[mascara, "tipo_cartao"] = df.loc[mascara, "tipo_cartao"].str.upper()

    return df


def _gravar_particao(df: pd.DataFrame, mes: pd.Period, raw_dir: Path) -> None:
    """Grava o mês como parquet em ``ref_month=YYYY-MM/`` (layout hive)."""
    particao = raw_dir / f"ref_month={mes.strftime('%Y-%m')}"
    particao.mkdir(parents=True, exist_ok=True)
    df.to_parquet(particao / "clientes.parquet", index=False)


def gerar_clientes(
    config: PipelineConfig | None = None, limit: int | None = None
) -> dict[str, Any]:
    """Gera o painel sintético e grava a camada ``raw`` particionada por mês.

    Args:
        config: configuração do pipeline (padrão: ``load_config()``).
        limit: reduz o número de clientes (modo amostra para CI e testes).

    Returns:
        Resumo com volumes e a comparação entre taxas de inadimplência
        sintética e real (a "prova de calibração" do gerador).
    """
    cfg = config or load_config()
    gen = cfg.generation
    n_clientes = gen.n_clientes if limit is None else min(limit, gen.n_clientes)
    rng = np.random.default_rng(gen.seed)

    base = dataset_para_geracao(cfg.paths)
    colunas_base = [c for c in base.columns if c not in ("id", "default")]
    taxa_real = float(base["default"].mean())

    # Propenso ao default (linha de origem inadimplente) entra mais vezes no default.
    multiplicador_medio = 1.0 + FATOR_RISCO * taxa_real
    p_entrada, taxa_real = _calibrar_p_entrada(taxa_real, multiplicador_medio)

    # Bootstrap com reposição: preserva a distribuição conjunta do real.
    amostra = base.sample(n=n_clientes, replace=True, random_state=rng).reset_index(drop=True)
    clientes = _construir_clientes(amostra, n_clientes, colunas_base)

    meses = pd.period_range(end=gen.mes_final, periods=gen.meses_historico, freq="M")

    n_linhas = 0
    soma_default = 0
    prev_default = clientes["default_base"].to_numpy()
    for indice_mes, mes in enumerate(meses):
        df_mes = _snapshot_mensal(
            clientes, prev_default, mes, indice_mes, p_entrada, rng, colunas_base
        )
        df_mes = aplicar_sujeira(df_mes, rng)

        soma_default += int(df_mes["default"].sum())
        n_linhas += len(df_mes)
        _gravar_particao(df_mes, mes, cfg.paths.raw)
        prev_default = df_mes["default"].to_numpy()
        if (indice_mes + 1) % 6 == 0 or indice_mes == len(meses) - 1:
            logger.info("Meses gravados: %d/%d (%d linhas)", indice_mes + 1, len(meses), n_linhas)

    taxa_sintetica = soma_default / n_linhas
    resumo = {
        "n_clientes": n_clientes,
        "n_meses": len(meses),
        "n_linhas": n_linhas,
        "taxa_default_sintetica": round(taxa_sintetica, 4),
        "taxa_default_real": round(taxa_real, 4),
        "diretorio": str(cfg.paths.raw),
    }
    logger.info(
        "Geração concluída: %d clientes x %d meses = %d linhas "
        "(taxa default sintética %.4f vs real %.4f)",
        n_clientes,
        len(meses),
        n_linhas,
        taxa_sintetica,
        taxa_real,
    )
    return resumo


def perfil_marginais(paths: Paths) -> dict[str, pd.Series]:
    """Marginais de categorias do dataset real (régua para os testes de calibração)."""
    df = dataset_para_geracao(paths)
    return {coluna: df[coluna].value_counts(normalize=True) for coluna in COLS_CATEGORICO}
