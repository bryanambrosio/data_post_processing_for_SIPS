# Automated Data Processing and Analysis Pipeline
Author: **Bryan Ambrósio**

This repository contains a complete pipeline for processing and analyzing `.PLT` files.  
`.PLT` files are simulation results obtianed via Organon software, wich is a software for simulatin power grid dinamics.
The steps cover everything from raw data preparation to the comparison between original and resampled signals.  

---

## 0. `0-run_pipeline.py`
Master script that runs all other scripts sequentially (1 → 6), with a short delay between them.  
Ensures the entire pipeline is executed automatically from start to finish.

---

## 1. `1-rename_plt_headers.py`
Reads `.PLT` files, applies variable name mapping (from Excel),  
and generates new versions with updated headers.

---

## 2. `2-plt_to_parquet.py`
Converts the renamed `.PLT` files to **Parquet** format, which is more efficient for analysis in Python.

---

## 3. `3-data_visualization.py`
Generates plots of the main variables grouped by prefix  
and saves them in `data_visualization/`.

---

## 4. `4-sampling_rate_evaluation.py`
Evaluates the **sampling rate** of the signals by calculating the average time step between samples (`deltaTempo`).

---

## 5. `5-interpol_resample_120Hz.py`
Resamples the signals to **120 Hz** using linear interpolation,  
producing new Parquet files with a regular time grid.

---

## 6. `6-compare_60hz_vs_120hz.py`
Compares original series (~60 Hz) with resampled ones (120 Hz),  
including detailed visualizations around the contingency event (~0.2 s).

---

## Directory Structure

When running the scripts, the following folder structure will be created automatically:

- `data_raw/`  
  Original `.PLT` files (raw input)

- `data_renamed/`  
  `.PLT` files with renamed headers

- `data_parquet/`  
  Files converted to Parquet format

- `data_parquet_120Hz/`  
  Parquet files resampled to 120 Hz

- `data_visualization/`  
  Generated plots from processed data

- `60hz_vs_120hz/`  
  Visual comparisons between original (≈60 Hz) and resampled (120 Hz) signals
