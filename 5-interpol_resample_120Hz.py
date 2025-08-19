# ================================================================
# Script: 5-interpol_resample_120Hz.py
# Autor: Bryan Ambrósio
# Descrição:
#   Reamostra arquivos .parquet para 120 Hz usando interpolação linear,
#   sem preservar explicitamente 0.2−/0.2+. 
#
# Entrada:  data_parquet/        (arquivos .parquet originais)
# Saída:    data_parquet_120Hz/  (reamostrados a 120 Hz)
# ================================================================

import os
import numpy as np
import pandas as pd

# ------------------------ Configuração --------------------------
PASTA_IN  = "data_parquet"
PASTA_OUT = "data_parquet_120Hz"
F_HZ      = 120.0
DT        = 1.0 / F_HZ           # ≈ 0.0083333333 s
os.makedirs(PASTA_OUT, exist_ok=True)
# ---------------------------------------------------------------

def gerar_grid(inicio: float, fim: float, passo: float) -> np.ndarray:
    """Gera grid regular [inicio, fim] com passo ~constante, incluindo o início."""
    if fim <= inicio:
        return np.array([], dtype=float)
    n = int(np.floor((fim - inicio) / passo)) + 1
    return inicio + np.arange(n, dtype=float) * passo

def processar_arquivo(caminho_in: str, caminho_out: str) -> None:
    df = pd.read_parquet(caminho_in, engine="pyarrow").reset_index(drop=True)

    # Detecta coluna de tempo
    col_t = next((c for c in df.columns if "tempo" in c.lower()), None)
    if col_t is None:
        print(f"⚠️  Coluna de tempo não encontrada → {os.path.basename(caminho_in)} (procuro substring 'tempo'). Pulando.")
        return

    # Garante tipo numérico e ordenação por tempo
    df[col_t] = pd.to_numeric(df[col_t], errors="coerce").astype("float64")
    df = df.dropna(subset=[col_t]).sort_values(col_t).reset_index(drop=True)

    # Remove duplicatas de timestamp (mantém a primeira)
    df = df.drop_duplicates(subset=[col_t], keep="first")

    # Seleciona apenas colunas numéricas para interpolação
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if col_t not in numeric_cols:
        numeric_cols.append(col_t)
    df_num = df[numeric_cols].copy()

    # Define base indexada por tempo
    base = df_num.set_index(col_t)

    # Gera grid regular em toda a extensão
    t_min = float(base.index.min())
    t_max = float(base.index.max())
    grid = gerar_grid(t_min, t_max, DT)
    if grid.size == 0:
        print(f"⚠️  Intervalo temporal inválido → {os.path.basename(caminho_in)}. Pulando.")
        return

    # Interpola linearmente em todas as colunas numéricas
    # (reindex union para preencher valores faltantes e depois selecionar o grid)
    base_interp = (
        base
        .reindex(base.index.union(grid))
        .interpolate(method="linear", limit_direction="both")
        .loc[grid]
        .reset_index()
        .rename(columns={"index": col_t})
    )

    # Se existirem colunas não numéricas no original, elas não são reamostradas.
    # (mantemos apenas as numéricas interpoladas + coluna de tempo)
    df_final = base_interp

    # Salva
    df_final.to_parquet(caminho_out, index=False)
    print(
        f"✅ {os.path.basename(caminho_in):<38} | "
        f"orig: {len(df):5d} → final(120Hz): {len(df_final):5d}"
    )

def main():
    arquivos = [f for f in os.listdir(PASTA_IN) if f.lower().endswith(".parquet")]
    if not arquivos:
        print(f"⚠️  Nenhum arquivo .parquet encontrado em {PASTA_IN}")
        return

    for nome in sorted(arquivos):
        src = os.path.join(PASTA_IN, nome)
        dst = os.path.join(PASTA_OUT, nome)
        try:
            processar_arquivo(src, dst)
        except Exception as e:
            print(f"❌ Erro em {nome}: {e}")

    print(f"\n🏁 Reamostragem concluída. Arquivos em: {PASTA_OUT}")

if __name__ == "__main__":
    main()
