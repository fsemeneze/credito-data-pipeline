"""Contrato de dados da camada trusted, executado com pandera.

O notebook original corrigia tipos, valores ausentes e categorias
estranhas manualmente, linha a linha, em células soltas. Aqui tudo isso
virou um contrato formal e executável:

* **coerção** (`transformation.coercir`) normaliza o dado raw para o
  domínio esperado (moeda BR vira float, caixa alta vira minúscula...);
* **validação** (`validar`) confere o contrato linha a linha e impede
  que o pipeline publique dado ruim: linhas que violam regras são
  rejeitadas e contadas; violação estrutural (coluna inteira fora do
  tipo) ou taxa de rejeição acima do limite interrompe o pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pandera.pandas as pa

from credito_pipeline.log import get_logger

logger = get_logger("validation")

# Limite de rejeição acima do qual o pipeline interrompe antes de publicar.
LIMITE_REJEICAO = 0.05

# Domínios observados no dataset real (régua de negócio do contrato).
DOMINIO_SEXO = ["f", "m"]
DOMINIO_ESCOLARIDADE = [
    "doutorado",
    "ensino fundamental",
    "ensino medio",
    "ensino superior",
    "graduacao",
    "mestrado",
    "sem educacao formal",
]
DOMINIO_ESTADO_CIVIL = ["casado", "divorciado", "solteiro"]
DOMINIO_SALARIO = ["$120k +", "$40k - $60k", "$60k - $80k", "$80k - $120k", "menos que $40k"]
DOMINIO_TIPO_CARTAO = ["blue", "gold", "platinum", "silver"]

COLS_CONTAGEM = (
    "default",
    "dependentes",
    "idade",
    "meses_de_relacionamento",
    "qtd_produtos",
    "iteracoes_12m",
    "meses_inativo_12m",
    "qtd_transacoes_12m",
)
COLS_CONTRATO: tuple[str, ...] = (
    "id",
    "default",
    "idade",
    "sexo",
    "dependentes",
    "escolaridade",
    "estado_civil",
    "salario_anual",
    "tipo_cartao",
    "meses_de_relacionamento",
    "qtd_produtos",
    "iteracoes_12m",
    "meses_inativo_12m",
    "limite_credito",
    "valor_transacoes_12m",
    "qtd_transacoes_12m",
    "ref_month",
)


class ViolacaoContratoErro(Exception):
    """Contrato violado de forma estrutural ou com taxa de rejeição acima do limite."""


@dataclass
class RelatorioValidacao:
    """Métricas de qualidade de uma execução da validação."""

    linhas_recebidas: int = 0
    duplicatas: int = 0
    valores_ausentes: int = 0
    violacoes: dict[str, int] = field(default_factory=dict)
    linhas_rejeitadas: int = 0
    linhas_validas: int = 0

    @property
    def taxa_rejeicao(self) -> float:
        """Proporção de linhas descartadas frente ao recebido."""
        if self.linhas_recebidas == 0:
            return 0.0
        return (self.linhas_rejeitadas + self.duplicatas) / self.linhas_recebidas

    def to_dict(self) -> dict[str, Any]:
        return {
            "linhas_recebidas": self.linhas_recebidas,
            "duplicatas": self.duplicatas,
            "valores_ausentes": self.valores_ausentes,
            "violacoes": self.violacoes,
            "linhas_rejeitadas": self.linhas_rejeitadas,
            "linhas_validas": self.linhas_validas,
            "taxa_rejeicao": round(self.taxa_rejeicao, 4),
        }


SCHEMA_TRUSTED = pa.DataFrameSchema(
    {
        "id": pa.Column("int64", pa.Check.gt(0), name="id_positivo"),
        "default": pa.Column("Int64", pa.Check.isin([0, 1]), name="default_binario", nullable=True),
        "idade": pa.Column(
            "Int64", pa.Check.in_range(18, 100), name="idade_plausivel", nullable=True
        ),
        "sexo": pa.Column(str, pa.Check.isin(DOMINIO_SEXO), name="sexo_dominio", nullable=True),
        "dependentes": pa.Column(
            "Int64", pa.Check.in_range(0, 20), name="dependentes_plausivel", nullable=True
        ),
        "escolaridade": pa.Column(
            str, pa.Check.isin(DOMINIO_ESCOLARIDADE), name="escolaridade_dominio", nullable=True
        ),
        "estado_civil": pa.Column(
            str, pa.Check.isin(DOMINIO_ESTADO_CIVIL), name="estado_civil_dominio", nullable=True
        ),
        "salario_anual": pa.Column(
            str, pa.Check.isin(DOMINIO_SALARIO), name="salario_dominio", nullable=True
        ),
        "tipo_cartao": pa.Column(
            str, pa.Check.isin(DOMINIO_TIPO_CARTAO), name="tipo_cartao_dominio", nullable=True
        ),
        "meses_de_relacionamento": pa.Column(
            "Int64", pa.Check.ge(0), name="relacionamento_plausivel", nullable=True
        ),
        "qtd_produtos": pa.Column(
            "Int64", pa.Check.in_range(1, 8), name="qtd_produtos_plausivel", nullable=True
        ),
        "iteracoes_12m": pa.Column(
            "Int64", pa.Check.in_range(0, 16), name="iteracoes_plausivel", nullable=True
        ),
        "meses_inativo_12m": pa.Column(
            "Int64", pa.Check.in_range(0, 12), name="inatividade_plausivel", nullable=True
        ),
        "limite_credito": pa.Column(
            "float64", pa.Check.ge(0.0), name="limite_nao_negativo", nullable=True
        ),
        "valor_transacoes_12m": pa.Column(
            "float64", pa.Check.ge(0.0), name="valor_nao_negativo", nullable=True
        ),
        "qtd_transacoes_12m": pa.Column(
            "Int64", pa.Check.in_range(0, 500), name="qtd_transacoes_plausivel", nullable=True
        ),
        "ref_month": pa.Column(
            str, pa.Check.str_matches(r"^\d{4}-\d{2}$"), name="ref_month_formato", nullable=True
        ),
    },
    strict=False,
    coerce=False,
)


def _casos_por_regra(failure_cases: pd.DataFrame) -> dict[str, int]:
    """Agrupa os casos de falha por coluna e regra (ex.: 'idade_pt: idade_plausivel')."""
    agrupado = failure_cases.groupby(["column", "check"], dropna=False).size()
    return {f"{coluna}::{regra}": int(qtd) for (coluna, regra), qtd in agrupado.items()}


def validar(df: pd.DataFrame) -> tuple[pd.DataFrame, RelatorioValidacao]:
    """Aplica o contrato e devolve o DataFrame trusted com o relatório.

    Linhas com regra violada (valores fora do domínio, tipos incoerentes,
    ausências) são descartadas e contadas no relatório. Qualquer
    violação estrutural (coluna inteira, não linha a linha) ou taxa de
    rejeição acima de ``LIMITE_REJEICAO`` levanta
    ``ViolacaoContratoErro`` — o pipeline para antes de publicar.
    """
    relatorio = RelatorioValidacao(linhas_recebidas=len(df))

    # Chave de painel: uma linha única por (id, ref_month); duplicata sai com keep='first'.
    duplicadas = df.duplicated(subset=["id", "ref_month"], keep="first")
    relatorio.duplicatas = int(duplicadas.sum())
    if relatorio.duplicatas:
        df = df.loc[~duplicadas]

    # Ausências: contadas por coluna, linhas marcadas para rejeição.
    indices_rejeitar: set[int] = set()
    ausentes = df[list(COLS_CONTRATO)].isna()
    por_linha = ausentes.any(axis=1)
    relatorio.valores_ausentes = int(por_linha.sum())
    indices_rejeitar.update(ausentes.index[por_linha.to_numpy()].tolist())

    # Contrato executável (pandera), lazy para capturar todas as regras de uma vez.
    try:
        SCHEMA_TRUSTED.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        casos = exc.failure_cases
        relatorio.violacoes = _casos_por_regra(casos)
        # Falha sem índice de linha é estrutural (dtype da coluna inteira,
        # nulabilidade etc.): não é espaçável, o pipeline para.
        puntuais = casos[casos["index"].notna()]
        estruturais = casos[casos["index"].isna()]
        if not estruturais.empty:
            raise ViolacaoContratoErro(
                f"violacao estrutural do contrato: {len(estruturais)} casos"
            ) from exc
        indices_rejeitar.update(puntuais["index"].astype(int).unique().tolist())

    if indices_rejeitar:
        df = df.drop(index=sorted(indices_rejeitar))
    relatorio.linhas_rejeitadas = len(indices_rejeitar)

    if relatorio.taxa_rejeicao > LIMITE_REJEICAO:
        raise ViolacaoContratoErro(
            f"taxa de rejeicao {relatorio.taxa_rejeicao:.2%} acima do limite de "
            f"{LIMITE_REJEICAO:.0%}; halting pipeline (relatório: {relatorio.to_dict()})"
        )

    # Após limpar violações e ausências, contagens ficam inteiras de verdade.
    df = df.copy()
    for coluna in COLS_CONTAGEM:
        df[coluna] = df[coluna].astype("int64")
    df = df.reset_index(drop=True)

    relatorio.linhas_validas = len(df)
    logger.info(
        "Validação: %d recebidas -> %d válidas (%d rejeitadas, %d duplicadas, rejeição %.2f%%)",
        relatorio.linhas_recebidas,
        relatorio.linhas_validas,
        relatorio.linhas_rejeitadas,
        relatorio.duplicatas,
        relatorio.taxa_rejeicao * 100,
    )
    return df, relatorio
