# Pipeline de Processamento e Análise Automatizada
Autor: **Bryan Ambrósio**

Este repositório contém uma pipeline completa para processamento e análise de arquivos `.PLT`.  
As etapas seguem da preparação dos dados brutos até a comparação entre sinais originais e reamostrados.  

---

## 0. `0-run_pipeline.py`
Script mestre que executa todos os outros scripts em sequência (1 → 6), com pequeno intervalo entre eles.  
Garante que o fluxo seja rodado de forma automática do início ao fim.

---

## 1. `1-rename_plt_headers.py`
Lê arquivos `.PLT`, aplica o mapeamento de nomes de variáveis (a partir de Excel)  
e gera versões com cabeçalhos atualizados.

---

## 2. `2-plt_to_parquet.py`
Converte os `.PLT` renomeados para formato **Parquet**, mais eficiente para análise em Python.

---

## 3. `3-data_visualization.py`
Cria gráficos das principais variáveis agrupadas por prefixo  
e salva em `data_visualization/`.

---

## 4. `4-sampling_rate_evaluation.py`
Avalia a **taxa de amostragem** dos sinais, calculando o passo médio entre amostras (`deltaTempo`).

---

## 5. `5-interpol_resample_120Hz.py`
Reamostra os sinais para **120 Hz** via interpolação linear,  
gerando novos arquivos Parquet com grade temporal regular.

---

## 6. `6-compare_60hz_vs_120hz.py`
Compara séries originais (~60 Hz) vs. reamostradas (120 Hz),  
incluindo visualizações detalhadas próximas da contingência (~0,2 s).
