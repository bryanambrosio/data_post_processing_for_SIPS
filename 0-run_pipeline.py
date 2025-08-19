# ================================================================
# Script: 0-run_pipeline.py
# Autor: Bryan Ambrósio
# Descrição:
#   Roda, em sequência, os scripts:
#     1-rename_plt_headers.py
#     2-plt_to_parquet.py
#     3-data_visualization.py
#     4-sampling_rate_evaluation.py
#     5-interpol_resample_120Hz.py
#     6-compare_60hz_vs_120hz.py
#   Espera um pequeno intervalo entre etapas.
# ================================================================

import sys
import time
import argparse
import subprocess
from pathlib import Path

SCRIPTS = [
    "1-rename_plt_headers.py",
    "2-plt_to_parquet.py",
    "3-data_visualization.py",
    "4-sampling_rate_evaluation.py",
    "5-interpol_resample_120Hz.py",
    "6-compare_60hz_vs_120hz.py",
]

def run_script(script_path: Path) -> int:
    """Executa um script Python com o mesmo interpretador, retornando o código de saída."""
    print(f"\n▶️  Rodando: {script_path.name}")
    print("-" * 72)
    # stdout/stderr = None -> herda do console e imprime em tempo real
    proc = subprocess.run([sys.executable, str(script_path)], cwd=script_path.parent)
    print("-" * 72)
    print(f"🔚 Finalizado: {script_path.name} (exit code={proc.returncode})")
    return proc.returncode

def main():
    parser = argparse.ArgumentParser(
        description="Roda a pipeline completa na ordem definida.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Tempo (em segundos) de espera entre scripts."
    )
    parser.add_argument(
        "--stop-on-error", action="store_true",
        help="Parar a pipeline no primeiro erro (exit code != 0)."
    )
    parser.add_argument(
        "--base-dir", type=str, default=".",
        help="Diretório onde estão os scripts."
    )
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()
    print(f"📂 Base dir: {base_dir}")
    print(f"⏱️  Delay entre etapas: {args.delay}s")
    print(f"⛔ Stop on error: {'SIM' if args.stop_on_error else 'NÃO'}")

    # Verificação de existência
    paths = []
    for name in SCRIPTS:
        p = base_dir / name
        if not p.exists():
            print(f"❌ Arquivo não encontrado: {p}")
            return 1
        paths.append(p)

    overall_ok = True
    for i, spath in enumerate(paths, start=1):
        code = run_script(spath)
        if code != 0:
            overall_ok = False
            print(f"❗ Script {spath.name} retornou código {code}.")
            if args.stop_on_error:
                print("🚫 Encerrando a pipeline por conta de erro.")
                return code
        if i < len(paths):
            time.sleep(args.delay)

    if overall_ok:
        print("\n✅ Pipeline concluída com sucesso!")
        return 0
    else:
        print("\n⚠️ Pipeline concluída com erros (veja mensagens acima).")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
