#!/usr/bin/env python3
# ================================================================
# Script: 6-compare_60hz_vs_120hz.py
# Autor: Bryan Ambrósio
# Descrição:
#   Compara séries de uma grandeza entre o arquivo original (amostragem
#   nativa) e o arquivo reamostrado/interpolado a 120 Hz.
#   Para cada par <nome>.parquet presente em data_parquet/ e
#   data_parquet_120Hz/, gera uma figura com 2 subplots:
#     (1) visão geral; (2) zoom em 0.15–0.45 s e limite superior de Y.
#   Figuras são salvas em data_visualization/.
# ================================================================

import os
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------- Configuração ----------------------------
GRANDEZA = "Vang_XES"          # nome da coluna a comparar
PASTA_ORIG = "data_parquet"
PASTA_120  = "data_parquet_120Hz"
PASTA_OUT  = "60hz_vs_120hz"

X_ZOOM = (0.15, 0.45)          # janela de zoom (s)
Y_MAX_ZOOM = 140               # limite superior do zoom (None para auto)
# ----------------------------------------------------------------

base = os.path.dirname(os.path.abspath(__file__))
pasta_original = os.path.join(base, PASTA_ORIG)
pasta_120hz    = os.path.join(base, PASTA_120)
outdir         = os.path.join(base, PASTA_OUT)
os.makedirs(outdir, exist_ok=True)

if not os.path.isdir(pasta_original):
    raise FileNotFoundError(f"Pasta não encontrada: {pasta_original}")
if not os.path.isdir(pasta_120hz):
    raise FileNotFoundError(f"Pasta não encontrada: {pasta_120hz}")

# Índice dos arquivos disponíveis em cada pasta
orig_set = {os.path.splitext(f)[0] for f in os.listdir(pasta_original) if f.lower().endswith(".parquet")}
hz_set   = {os.path.splitext(f)[0] for f in os.listdir(pasta_120hz)   if f.lower().endswith(".parquet")}

# Pares com o mesmo nome base
bases_em_comum = sorted(orig_set & hz_set)
if not bases_em_comum:
    raise FileNotFoundError("Nenhum par <nome>.parquet encontrado simultaneamente em data_parquet/ e data_parquet_120Hz/.")

def detectar_coluna_tempo(df: pd.DataFrame) -> str | None:
    return next((c for c in df.columns if "tempo" in c.lower()), None)

for nome_base in bases_em_comum:
    caminho_original = os.path.join(pasta_original, f"{nome_base}.parquet")
    caminho_interp   = os.path.join(pasta_120hz,   f"{nome_base}.parquet")

    try:
        df_original = pd.read_parquet(caminho_original, engine="pyarrow")
        df_interp   = pd.read_parquet(caminho_interp,   engine="pyarrow")
    except Exception as e:
        print(f"❌ Erro lendo '{nome_base}': {e}")
        continue

    col_tempo_original = detectar_coluna_tempo(df_original)
    col_tempo_interp   = detectar_coluna_tempo(df_interp)

    if col_tempo_original is None or col_tempo_interp is None:
        print(f"⚠️ Coluna de tempo ausente em '{nome_base}'. Pulando.")
        continue

    if GRANDEZA not in df_original.columns or GRANDEZA not in df_interp.columns:
        print(f"⚠️ Grandeza '{GRANDEZA}' não encontrada em ambos para '{nome_base}'. Pulando.")
        continue

    # Figura com dois subplots independentes
    fig, axs = plt.subplots(2, 1, figsize=(14, 8))

    # Plot 1: visão geral
    axs[0].plot(
        df_original[col_tempo_original], df_original[GRANDEZA],
        label="Original",
        color="#666666", marker='o', linestyle='-', markersize=6, markerfacecolor='none'
    )
    axs[0].plot(
        df_interp[col_tempo_interp], df_interp[GRANDEZA],
        label="Interpolado/Reamostrado (120 Hz)",
        color="#e57373", marker='^', linestyle='--', markersize=6, markerfacecolor='none'
    )
    axs[0].set_title(f"{nome_base} — Simulação vs. Interpolado/Reamostrado (120 Hz)")
    axs[0].set_xlabel("Tempo (s)")
    axs[0].set_ylabel("Diferença Angular Xingu–Estreito (°)")
    axs[0].grid(True)
    axs[0].legend()

    # Plot 2: zoom
    axs[1].plot(
        df_original[col_tempo_original], df_original[GRANDEZA],
        label="Original",
        color="#666666", marker='o', linestyle='-', markersize=6, markerfacecolor='none'
    )
    axs[1].plot(
        df_interp[col_tempo_interp], df_interp[GRANDEZA],
        label="Interpolado/Reamostrado (120 Hz)",
        color="#e57373", marker='^', linestyle='--', markersize=6, markerfacecolor='none'
    )
    axs[1].set_xlim(*X_ZOOM)
    if Y_MAX_ZOOM is not None:
        axs[1].set_ylim(top=Y_MAX_ZOOM)
    axs[1].set_title("Zoom")
    axs[1].set_xlabel("Tempo (s)")
    axs[1].set_ylabel("Diferença Angular Xingu–Estreito (°)")
    axs[1].grid(True)
    axs[1].legend()

    plt.tight_layout()

    # Salvar e fechar (não mostrar na tela)
    out_png = os.path.join(outdir, f"{nome_base}__{GRANDEZA}__orig_vs_120Hz.png")
    plt.savefig(out_png, dpi=150)
    plt.close()
    print(f"✅ Figura salva: {out_png}")

print("\n🏁 Concluído. Verifique as figuras em data_visualization/")
