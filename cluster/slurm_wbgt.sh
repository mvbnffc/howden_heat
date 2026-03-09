#!/bin/bash
#SBATCH --job-name=wbgt_process
#SBATCH --array=1983-2016          # one job per year — adjust range as needed
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G                   # ~8 GB is sufficient for a single year
#SBATCH --time=04:00:00            # 4 hours per year is generous
#SBATCH --output=logs/wbgt_%a.out
#SBATCH --error=logs/wbgt_%a.err
#SBATCH --partition=standard       # adjust to your cluster's partition name

# ---------------------------------------------------------------------------
# Configuration — edit these paths before submitting
# ---------------------------------------------------------------------------
PROJECT_DIR="/path/to/howden_heat"          # absolute path to project root
CONDA_ENV="howden_heat"
OUT_DIR="${PROJECT_DIR}/data/processed/annual"
CONFIG="${PROJECT_DIR}/config/config.yaml"
# ---------------------------------------------------------------------------

mkdir -p "${PROJECT_DIR}/logs"
mkdir -p "${OUT_DIR}"

# Activate conda environment
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${CONDA_ENV}"

YEAR=${SLURM_ARRAY_TASK_ID}

echo "$(date) — Starting year ${YEAR} on $(hostname)"

python "${PROJECT_DIR}/scripts/process_wbgt.py" \
    --year "${YEAR}" \
    --out-dir "${OUT_DIR}" \
    --config "${CONFIG}" \
    --skip-existing

EXIT_CODE=$?
echo "$(date) — Finished year ${YEAR} with exit code ${EXIT_CODE}"
exit ${EXIT_CODE}
