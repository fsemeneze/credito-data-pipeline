"""Testes das camadas de ingestão, validação, transformação e carga (fase 1).

Reuso do helper de config de ``test_generation`` (mesmo raw real,
camadas gravadas em tmp). Os testes aqui cobrem: coerção, o contrato
pandera (rejeição linhas a linhas, gate de taxa, violação estrutural),
features da curated e particionamento da carga.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from credito_pipeline import load as camada_load
from credito_pipeline import transformation
from credito_pipeline.generation import gerar_clientes
from credito_pipeline.ingestion import ingestar
from credito_pipeline.validation import RelatorioValidacao, ViolacaoContratoErro, validar

try:
    from test_generation import _config  # type: ignore[no-redef]
except ImportError:  # quando executado como pacote (python -m pytest)
    from tests.test_generation import _config  # type: ignore[no-redef]


def _df_base(n: int = 100) -> pd.DataFrame:
    """DataFrame coerçado válido, uma linha por id, para os testes de contrato."""
    meses = ["2026-08", "2026-09"]
    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "default": [0] * n,
            "idade": [40] * n,
            "sexo": ["M"] * n,
            "dependentes": [2] * n,
            "escolaridade": ["mestrado"] * n,
            "estado_civil": ["casado"] * n,
            "salario_anual": ["$40K - $60K"] * n,
            "tipo_cartao": ["blue"] * n,
            "meses_de_relacionamento": [30] * n,
            "qtd_produtos": [2] * n,
            "iteracoes_12m": [3] * n,
            "meses_inativo_12m": [1] * n,
            "limite_credito": [5000.0] * n,
            "valor_transacoes_12m": [1000.0] * n,
            "qtd_transacoes_12m": [50] * n,
            "ref_month": [meses[i % 2] for i in range(n)],
        }
    )


def test_ponta_a_ponta_rede_das_camadas(tmp_path):
    cfg = _config(tmp_path, n=20_000, meses=3)

    gerar_clientes(cfg)
    raw = ingestar(cfg)
    coerced = transformation.coercir(raw)
    trusted, relatorio = validar(coerced)
    curated = transformation.enriquecer(trusted)

    assert relatorio.linhas_recebidas == len(raw)
    assert relatorio.taxa_rejeicao < 0.05
    # Balanço do relatório: recebidas = válidas + rejeitadas + duplicatas.
    assert relatorio.linhas_validas + relatorio.linhas_rejeitadas + relatorio.duplicatas == (
        relatorio.linhas_recebidas
    )
    assert curated["flag_default_historico"].max() in (0, 1)

    resumo = camada_load.carregar(trusted, cfg, camada="trusted", relatorio=relatorio.to_dict())
    assert resumo["meses"] == cfg.generation.meses_historico
    particoes = list((cfg.paths.trusted).glob("ref_month=*/dados.parquet"))
    assert len(particoes) == cfg.generation.meses_historico

    arq_metrica = cfg.paths.metrics / "validacao_trusted.json"
    assert arq_metrica.exists()
    metrica = json.loads(arq_metrica.read_text(encoding="utf-8"))
    assert metrica["pipeline"]["linhas_recebidas"] == len(raw)


def test_coercao_converte_moeda_e_normaliza_categorias():
    df = pd.DataFrame(
        {
            "limite_credito": ["1.234,56", "na"],
            "valor_transacoes_12m": ["4.224,85", "0,00"],
            "sexo": [" M", "f "],
            "dependentes": [2, 1],
            "idade": [40, 40],
            "meses_de_relacionamento": [30, 30],
            "qtd_produtos": [2, 2],
            "iteracoes_12m": [3, 3],
            "meses_inativo_12m": [1, 1],
            "default": [0, 1],
            "qtd_transacoes_12m": [50, 60],
            "tipo_cartao": ["BLUE", "BLUE"],
            "escolaridade": ["mestrado", "graduacao"],
            "estado_civil": ["casado", "solteiro"],
            "salario_anual": ["$40K - $60K", "$60K - $80K"],
        }
    )
    coerced = transformation.coercir(df)

    assert coerced["limite_credito"].iloc[0] == pytest.approx(1234.56)
    assert pd.isna(coerced["limite_credito"].iloc[1])
    assert list(coerced["tipo_cartao"]) == ["blue", "blue"]
    assert list(coerced["sexo"]) == ["m", "f"]


def test_validacao_rejeita_e_relata_por_regra():
    df = _df_base(100)
    df.loc[0, "default"] = 2  # fora do domínio binário
    df.loc[1, "idade"] = 17  # menor de idade
    df.loc[2, "limite_credito"] = -5.0  # monetário negativo
    df.loc[3, "salario_anual"] = None  # ausência
    linha_dup = df.iloc[[0]]  # (id, ref_month) repetido
    df = pd.concat([df, linha_dup], ignore_index=True)

    coerced = transformation.coercir(df)
    trusted, relatorio = validar(coerced)

    assert relatorio.linhas_recebidas == 101
    assert relatorio.duplicatas == 1
    assert relatorio.linhas_rejeitadas == 4
    assert relatorio.taxa_rejeicao == pytest.approx(5 / 101, abs=1e-3)
    assert relatorio.linhas_validas == 96
    regras = " ".join(relatorio.violacoes)
    assert "default::isin" in regras
    assert "idade::in_range" in regras
    assert "limite_credito::greater_than_or_equal_to" in regras
    assert relatorio.valores_ausentes == 1
    # Linha de id=1 tem idade 17, então é rejeitada pela regra de idade.


def test_validacao_gate_de_taxa():
    df = _df_base(100)
    df.loc[:, "idade"] = 17  # 100% das linhas violam a mesma regra

    with pytest.raises(ViolacaoContratoErro, match="limite"):
        validar(df)


def test_validacao_falha_estrutural():
    # Coluna inteira com float em vez de int (dtype do contrato violado ao todo).
    df = _df_base(100).astype({"default": "float64"})

    with pytest.raises(ViolacaoContratoErro, match="estrutural"):
        validar(df)


def test_enriquecimento_features_cumulativas_por_cliente():
    trusted = _df_base(4)
    trusted["id"] = [1, 1, 2, 2]
    trusted["ref_month"] = ["2026-08", "2026-09", "2026-08", "2026-09"]
    trusted["default"] = [0, 1, 0, 0]
    trusted["valor_transacoes_12m"] = [100.0, 200.0, 10.0, 20.0]

    curated = transformation.enriquecer(trusted)
    curated = curated.set_index(["id", "ref_month"])[
        ["flag_default_historico", "valor_total_acumulado"]
    ]

    assert curated.loc[(1, "2026-08"), "flag_default_historico"] == 0
    assert curated.loc[(1, "2026-09"), "flag_default_historico"] == 1  # cummax conta o default
    assert curated.loc[(2, "2026-09"), "flag_default_historico"] == 0
    assert curated.loc[(1, "2026-09"), "valor_total_acumulado"] == pytest.approx(300.0)
    assert curated.loc[(2, "2026-09"), "valor_total_acumulado"] == pytest.approx(30.0)


def test_carga_exige_ref_month(tmp_path):
    cfg = _config(tmp_path, n=10, meses=1)

    df_sem_particao = pd.DataFrame({"id": [1]})
    with pytest.raises(ValueError, match="ref_month"):
        camada_load.carregar(df_sem_particao, cfg, camada="trusted")


def test_relatorio_vazio_nao_rejeita():
    relatorio = RelatorioValidacao()
    assert relatorio.taxa_rejeicao == 0.0
