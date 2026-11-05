"""Testes do gerador sintético (fase 1).

Duas famílias de garantias:

1. **Contrato estrutural**: colunas, volumes, chave única e domínio do default.
2. **Calibração estatística**: o sintético reproduz a taxa de inadimplência
   do dataset real dentro de uma tolerância declarada.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from credito_pipeline import generation
from credito_pipeline.config import GenerationConfig, Paths, PipelineConfig
from credito_pipeline.generation import aplicar_sujeira, gerar_clientes

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _config(tmp_path: Path, n: int = 50_000, meses: int = 6) -> PipelineConfig:
    """Config de teste: leitura do seed real, gravação das camadas em tmp.

    ``n`` grande a ponto de a taxa média do painel não sofrer variação
    de amostragem (σ ≈ 0,002 por cliente) e os testes ficarem robustos.
    """
    paths = Paths(
        root=PROJECT_ROOT,
        data=PROJECT_ROOT / "data",
        raw=tmp_path / "raw",
        trusted=tmp_path / "trusted",
        curated=tmp_path / "curated",
        metrics=tmp_path / "metrics",
    )
    gen = GenerationConfig(n_clientes=n, meses_historico=meses, mes_final="2026-09", seed=7)
    return PipelineConfig(paths=paths, generation=gen)


def _painel_completo(paths: Paths) -> pd.DataFrame:
    particoes = sorted((paths.raw).glob("ref_month=*/clientes.parquet"))
    return pd.concat((pd.read_parquet(p) for p in particoes), ignore_index=True)


def test_geracao_produz_painel_completo(tmp_path):
    cfg = _config(tmp_path)
    resumo = gerar_clientes(cfg)

    assert resumo["n_linhas"] == resumo["n_clientes"] * resumo["n_meses"]
    particoes = sorted(p.name for p in cfg.paths.raw.iterdir())
    assert len(particoes) == cfg.generation.meses_historico
    assert particoes[0].startswith("ref_month=")


def test_schema_sintetico_igual_ao_real(tmp_path):
    cfg = _config(tmp_path)
    gerar_clientes(cfg)

    df = _painel_completo(cfg.paths)
    real = pd.read_csv(cfg.paths.seed_dataset, nrows=0)

    assert [c for c in df.columns if c != "ref_month"] == list(real.columns)


def test_chave_unica_e_default_binario(tmp_path):
    cfg = _config(tmp_path)
    gerar_clientes(cfg)

    df = _painel_completo(cfg.paths)
    assert not df.duplicated(["id", "ref_month"]).any()
    assert set(df["default"].unique()) <= {0, 1}
    assert (df["idade"] >= 18).all()

    # Moeda chega como texto no formato da fonte; numericamente não pode ser negativa.
    from credito_pipeline.calibration import parse_moeda_br

    valores = df["valor_transacoes_12m"].map(parse_moeda_br)
    assert valores.dropna().ge(0).all()


def test_taxa_default_reproduz_a_real(tmp_path):
    cfg = _config(tmp_path)
    resumo = gerar_clientes(cfg)

    diferenca = abs(resumo["taxa_default_sintetica"] - resumo["taxa_default_real"])
    assert diferenca < 0.02, (
        f"sintética {resumo['taxa_default_sintetica']} vs real {resumo['taxa_default_real']}"
    )


def test_id_evolui_e_relacionamento_cresce(tmp_path):
    cfg = _config(tmp_path)
    gerar_clientes(cfg)

    df = _painel_completo(cfg.paths)
    df = df.sort_values(["id", "ref_month"])
    idade_mensal = df.groupby("id")["idade"].diff().dropna()
    relacionamento = df.groupby("id")["meses_de_relacionamento"].diff().dropna()

    assert (idade_mensal == idade_mensal).all()  # sem NaN na evolução
    assert (idade_mensal.abs() <= 1).all()  # no máximo +1 por ano (12 meses)
    assert (relacionamento == 1).all()


def test_sujeira_produz_imperfeicoes(monkeypatch):
    # Proporções 100% tornam o teste determinístico.
    monkeypatch.setattr(generation, "PROP_VALOR_NAO_DISPONIVEL", 1.0)
    monkeypatch.setattr(generation, "PROP_CATEGORIA_FORA_PADRAO", 1.0)

    df = pd.DataFrame(
        {
            "limite_credito": ["1.000,00"] * 200,
            "valor_transacoes_12m": ["500,00"] * 200,
            "qtd_transacoes_12m": [10] * 200,
            "escolaridade": ["mestrado"] * 200,
            "estado_civil": ["casado"] * 200,
            "tipo_cartao": ["blue"] * 200,
        }
    )
    sujo = aplicar_sujeira(df, np.random.default_rng(0))

    ausencias = sujo["escolaridade"].isna().any() and sujo["qtd_transacoes_12m"].isna().any()
    fora_padrao = (sujo["tipo_cartao"] == "BLUE").any()

    assert ausencias
    assert fora_padrao

    # O dado limpo chega intacto (a sujeira é pontual, por linha).
    moeda_limpa = sujo["valor_transacoes_12m"].dropna()
    assert moeda_limpa.map(lambda v: isinstance(v, str) and "," in v).all()
