#!/usr/bin/env python3
# ================================================================
# Script: 1-rename_plt_headers.py
# Autor: Bryan Ambrósio
# Descrição:
#   Renomeia variáveis no cabeçalho de arquivos .PLT,
#   utilizando mapeamento definido em um arquivo Excel.
#
#   Formato do arquivo .PLT:
#     - Linha 1: número total de variáveis (incluindo tempo)
#     - Linhas 2 a N+1: nome de cada variável, uma por linha
#     - Demais linhas: dados
#
# Entrada:
#   - Pasta: data_raw/
#   - Arquivo Excel: mudança_nomes_variaveis_cabeçalho.xlsx
#
# Saída:
#   - Pasta: data_renamed/
#
# ================================================================

import os
import pandas as pd


def load_mapping(
    excel_path: str,
    orig_col: str = "Nome Atual",
    new_col: str = "Nome Atualizado"
) -> dict[str, str]:
    """
    Carrega dicionário de mapeamento {nome_original: nome_atualizado} do Excel.
    """
    df = pd.read_excel(excel_path, dtype=str).fillna("")
    mapping: dict[str, str] = {}
    for _, row in df.iterrows():
        orig = str(row[orig_col]).strip()
        new  = str(row[new_col]).strip()
        if orig:
            mapping[orig] = new or orig
    return mapping


def rename_plt(path_in: str, path_out: str, mapping: dict[str, str]) -> None:
    """
    Renomeia as variáveis do cabeçalho de um .PLT com base em mapping.
    Preserva a linha do tempo (primeira variável).
    """
    with open(path_in, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    if len(lines) < 2:
        raise ValueError(f"Arquivo muito curto: {path_in}")

    # 1ª linha: número total de variáveis
    try:
        n_vars = int(lines[0].strip())
    except ValueError:
        raise ValueError(
            f"Linha 1 não é inteiro em {path_in}: '{lines[0].strip()}'"
        )

    # Verifica se há cabeçalho completo
    if len(lines) < 1 + n_vars + 1:
        raise ValueError(
            f"Esperado {n_vars} variáveis, mas arquivo tem apenas {len(lines)-1} linhas de header em {path_in}"
        )

    # Extrai cabeçalho original (sem a contagem)
    header_original = [ln.rstrip("\n") for ln in lines[1: 1 + n_vars]]

    # Reconstrói novo cabeçalho
    new_header: list[str] = []
    for idx, var in enumerate(header_original):
        if idx == 0:
            # Preserva tempo
            new_header.append(var)
        else:
            key = var.strip()
            if key in mapping:
                new_header.append(mapping[key])
            else:
                print(f"⚠ Sem mapeamento para '{key}' em {os.path.basename(path_in)}")
                new_header.append(key)

    # Garante diretório de saída
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    with open(path_out, 'w', encoding='utf-8') as f:
        # Regrava contagem
        f.write(lines[0].strip() + "\n")
        # Grava novo cabeçalho
        for var in new_header:
            f.write(var + "\n")
        # Grava dados remanescentes
        f.writelines(lines[1 + n_vars:])


def main() -> None:
    base = os.path.dirname(os.path.abspath(__file__))
    in_dir = os.path.join(base, 'data_raw')        # <- alterado
    out_dir = os.path.join(base, 'data_renamed')   # <- alterado
    excel_map = os.path.join(
        base,
        'mudança_nomes_variaveis_cabeçalho.xlsx'
    )

    if not os.path.isdir(in_dir):
        raise FileNotFoundError(f"Pasta de entrada não encontrada: {in_dir}")

    mapping = load_mapping(excel_map)
    print(f"🔍 {len(mapping)} mapeamentos carregados")

    os.makedirs(out_dir, exist_ok=True)
    for fname in sorted(os.listdir(in_dir)):
        if not fname.lower().endswith('.plt'):
            continue
        src = os.path.join(in_dir, fname)
        dst = os.path.join(out_dir, fname)
        try:
            rename_plt(src, dst, mapping)
            print(f"✅ {fname} renomeado")
        except Exception as e:
            print(f"❌ Erro em {fname}: {e}")

if __name__ == '__main__':
    main()
