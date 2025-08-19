#!/usr/bin/env python3
# ================================================================
# Script: 4-sampling_rate_evaluation.py
# Autor: Bryan Ambrósio
# Descrição:
#   Lê todos os .parquet em data_parquet/, detecta a coluna de tempo,
#   calcula o deltaTempo entre amostras consecutivas e salva um
#   gráfico em data_visualization/ para cada arquivo.
# ================================================================

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

base = os.path.dirname(os.path.abspath(__file__))
pasta_in = os.path.join(base, "data_parquet")
pasta_out = os.path.join(base, "data_visualization")
os.makedirs(pasta_out, exist_ok=True)

arquivos = [f for f in os.listdir(pasta_in) if f.lower().endswith(".parquet")]
if not arquivos:
    raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {pasta_in}")

for nome_arq in sorted(arquivos):
    caminho_parquet = os.path.join(pasta_in, nome_arq)
    print(f"📄 Processando {nome_arq}...")

    # Lê o arquivo parquet
    df = pd.read_parquet(caminho_parquet, engine="pyarrow")

    # Detecta a coluna de tempo
    coluna_tempo = next((c for c in df.columns if "tempo" in c.lower()), None)
    if coluna_tempo is None:
        print(f"⚠️ Coluna de tempo não encontrada em {nome_arq}. Pulando.")
        continue

    # Extrai vetor de tempo
    tempo = pd.to_numeric(df[coluna_tempo], errors="coerce").to_numpy()
    tempo = tempo[~np.isnan(tempo)]

    if tempo.size < 2:
        print(f"⚠️ Arquivo {nome_arq} tem menos de 2 amostras válidas. Pulando.")
        continue

    # Calcula deltaTempo
    deltaTempo = np.diff(tempo)

    # Estatísticas básicas
    media = float(np.mean(deltaTempo))
    print(f"   Média deltaTempo = {media:.9f} s ({1/media:.2f} Hz aprox.)")

    # Gera gráfico
    plt.figure(figsize=(10, 4))
    plt.plot(tempo[1:], deltaTempo, marker="o", linewidth=1)
    plt.title("Passo de Tempo entre Amostras (deltaTempo)")
    plt.xlabel("Tempo (s)")
    plt.ylabel("deltaTempo (s)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()

    # Salva gráfico
    nome_base = os.path.splitext(nome_arq)[0]
    png_out = os.path.join(pasta_out, f"{nome_base}__deltaTempo.png")
    plt.savefig(png_out, dpi=150)
    plt.close()
    print(f"   ✅ Gráfico salvo em {png_out}")

print("\n🏁 Concluído. Gráficos disponíveis em data_visualization/")
