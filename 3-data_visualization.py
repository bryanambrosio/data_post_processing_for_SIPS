# ================================================================
# Script: 3-data_visualization.py
# Autor: Bryan Ambrósio
# Descrição:
#   Lê .parquet(s) de data_parquet/, detecta a coluna de tempo e
#   plota variáveis por prefixo, salvando em data_visualization/.
#   Inclui verificações e logs para diagnosticar erros de leitura.
# ================================================================

import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# ---------- Configuração ----------
SOMENTE_UM_ARQUIVO = False  # True para processar só UM arquivo específico
NOME_ARQUIVO_UNICO = "PCC_1500_PO1_DIR1_1_EVT_1.parquet"  # usado se SOMENTE_UM_ARQUIVO=True

PREFIXOS = [
    "Rang", "Vpu", "Vang", "Idpu", "Pmpu", "Prpu", "Freqpu", "dFreqpus", "FN"
]
# ----------------------------------

base = os.path.dirname(os.path.abspath(__file__))
pasta_in = os.path.join(base, "data_parquet")
pasta_out = os.path.join(base, "data_visualization")
os.makedirs(pasta_out, exist_ok=True)

def listar_parquets(pasta: str) -> list[str]:
    return sorted(glob.glob(os.path.join(pasta, "*.parquet")))

def detectar_coluna_tempo(df: pd.DataFrame) -> str | None:
    return next((c for c in df.columns if "tempo" in c.lower()), None)

def grupos_por_prefixo(colunas: list[str]) -> dict[str, list[str]]:
    grupos = {}
    for pref in PREFIXOS:
        grupos[pref] = [c for c in colunas if c.startswith(pref)]
    return grupos

def processar_parquet(caminho_parquet: str) -> None:
    nome_base = os.path.splitext(os.path.basename(caminho_parquet))[0]
    print(f"\n📄 Processando: {caminho_parquet}")

    if not os.path.isfile(caminho_parquet):
        print(f"❌ Arquivo não encontrado: {caminho_parquet}")
        return

    try:
        # engine="pyarrow" é o padrão recomendado
        df = pd.read_parquet(caminho_parquet, engine="pyarrow")
    except Exception as e:
        print("❌ Falha ao ler Parquet.")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensagem: {e}")
        print("   Dicas:")
        print("   - Verifique se 'pyarrow' está instalado (pip install pyarrow).")
        print("   - Confirme se o arquivo não está corrompido (tente abrir outro .parquet).")
        print("   - Confirme o caminho impresso acima.")
        return

    col_tempo = detectar_coluna_tempo(df)
    if col_tempo is None:
        print("❌ Coluna de tempo não encontrada (procuro por substring 'tempo' em df.columns).")
        print(f"   Colunas disponíveis: {list(df.columns)[:15]}{'...' if len(df.columns)>15 else ''}")
        return

    tempo = df[col_tempo]
    grupos = grupos_por_prefixo(list(df.columns))

    for nome_grupo, colunas in grupos.items():
        if not colunas:
            continue
        plt.figure(figsize=(12, 6))
        for coluna in colunas:
            try:
                plt.plot(tempo, df[coluna], label=coluna)
            except Exception as e:
                print(f"   ⚠️ Erro ao plotar coluna '{coluna}': {e}")
        plt.xlabel("Tempo (s)")
        plt.ylabel("Valor")
        plt.title(f"{nome_base} — grupo {nome_grupo}")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        outfile = os.path.join(pasta_out, f"{nome_base}__{nome_grupo}.png")
        plt.savefig(outfile, dpi=150)
        plt.close()
        print(f"   ✅ Salvo: {outfile}")

    print(f"🏁 Concluído para: {nome_base}")

def main():
    print(f"📂 Pasta de entrada: {pasta_in}")
    print(f"📂 Pasta de saída:   {pasta_out}")
    if not os.path.isdir(pasta_in):
        print("❌ Pasta de entrada não existe.")
        return

    arquivos = listar_parquets(pasta_in)
    print(f"🔍 Encontrados {len(arquivos)} arquivo(s) .parquet.")

    if SOMENTE_UM_ARQUIVO:
        alvo = os.path.join(pasta_in, NOME_ARQUIVO_UNICO)
        print(f"🎯 Modo arquivo único: {alvo}")
        processar_parquet(alvo)
    else:
        if not arquivos:
            print("⚠️ Nenhum .parquet encontrado em data_parquet/.")
            return
        for caminho in arquivos:
            processar_parquet(caminho)

if __name__ == "__main__":
    main()
