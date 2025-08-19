# ================================================================
# Script: 2-plt_to_parquet.py
# Autor: Bryan Ambrósio
# Descrição:
#   Converte arquivos .PLT (já renomeados) em arquivos .parquet
#   usando reshape manual.
#
# Fluxo:
#   1) Lê n_vars (número total de variáveis) na 1ª linha.
#   2) Lê as próximas n_vars linhas como cabeçalho (inclui tempo).
#   3) Coleta os dados brutos, separa por espaços e concatena valores.
#   4) Verifica múltiplo de n_vars e faz reshape(-1, n_vars).
#   5) Gera DataFrame com nomes de variáveis, garantindo unicidade.
#   6) Salva em Parquet.
#
# Entrada:  data_renamed/    (arquivos .plt)
# Saída:    data_parquet/    (arquivos .parquet)
# ================================================================

import os
import pandas as pd
import numpy as np


def tornar_colunas_unicas(colunas: list[str]) -> list[str]:
    """Caso existam duplicatas, adiciona sufixos _1, _2, ..."""
    cont: dict[str, int] = {}
    resultado: list[str] = []
    for c in colunas:
        if c in cont:
            cont[c] += 1
            resultado.append(f"{c}_{cont[c]}")
        else:
            cont[c] = 0
            resultado.append(c)
    return resultado


def ler_plt_como_tabela(caminho_arquivo: str) -> pd.DataFrame:
    """
    Lê o arquivo .PLT e retorna um DataFrame:
    - Linha 1: n_vars (int)
    - Linhas 2..(n_vars+1): nomes das variáveis (inclui tempo)
    - Demais linhas: dados separados por espaços
    """
    with open(caminho_arquivo, 'r', encoding='utf-8', errors='ignore') as f:
        linhas = [ln.rstrip('\n') for ln in f]

    if len(linhas) < 2:
        raise ValueError(f"Arquivo muito curto: {caminho_arquivo}")

    # 1) número de variáveis
    try:
        n_vars = int(linhas[0].strip())
    except ValueError:
        raise ValueError(f"Não foi possível ler n_vars na 1ª linha de {caminho_arquivo}: '{linhas[0]}'")

    # 2) cabeçalho completo
    if len(linhas) < 1 + n_vars + 1:
        raise ValueError(
            f"Esperado {n_vars} variáveis no header, mas só há {len(linhas)-1} linhas após a contagem em {caminho_arquivo}"
        )
    header_lines = [h.strip() for h in linhas[1: 1 + n_vars]]
    header = tornar_colunas_unicas(header_lines)

    # 3) dados brutos
    raw = linhas[1 + n_vars:]
    valores: list[str] = []
    for ln in raw:
        if not ln.strip():
            continue
        valores.extend(ln.split())

    total = len(valores)
    if total % n_vars != 0:
        raise ValueError(
            f"Valores ({total}) não é múltiplo de n_vars ({n_vars}) em {caminho_arquivo}"
        )

    # 4) reshape e DataFrame
    arr = np.array(valores, dtype=float).reshape(-1, n_vars)
    df = pd.DataFrame(arr, columns=header)
    return df


def main() -> None:
    base = os.path.dirname(os.path.abspath(__file__))
    pasta_in = os.path.join(base, 'data_renamed')   # <- entrada
    pasta_out = os.path.join(base, 'data_parquet')   # <- saída
    os.makedirs(pasta_out, exist_ok=True)

    if not os.path.isdir(pasta_in):
        raise FileNotFoundError(f"Pasta de entrada não encontrada: {pasta_in}")

    arquivos = [f for f in os.listdir(pasta_in) if f.lower().endswith('.plt')]
    print(f"🔍 {len(arquivos)} arquivos .plt em {pasta_in}")

    for arq in sorted(arquivos):
        src = os.path.join(pasta_in, arq)
        dst = os.path.join(pasta_out, os.path.splitext(arq)[0] + '.parquet')
        print(f"📄 Processando {arq}...")
        try:
            df = ler_plt_como_tabela(src)
            # Você pode adicionar compression='snappy' se desejar compactar:
            df.to_parquet(dst, index=False)
            print(f"✅ {arq} → {dst}")
        except Exception as e:
            print(f"⚠️ Erro em {arq}: {e}")

    print(f"\n🏁 Concluído. Verifique os parquets em {pasta_out}")


if __name__ == '__main__':
    main()
