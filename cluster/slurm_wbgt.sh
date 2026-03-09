#!/bin/bash
# ---------------------------------------------------------------------------
# SLURM job array — process one year per task.
#
# Called by submit_all.sh with --export to set DATASET, FUTURE_EPOCH,
# SCENARIO, START_YEAR, END_YEAR. Do not submit this script directly;
# use submit_all.sh instead.
# ---------------------------------------------------------------------------
#SBATCH --job-name=wbgt_process
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=04:00:00
#SBATCH --output=logs/wbgt_%x_%a.out
#SBATCH --error=logs/wbgt_%x_%a.err

# ---------------------------------------------------------------------------
# These are set by submit_all.sh via --export — do not edit here
# DATASET, FUTURE_EPOCH, SCENARIO, START_YEAR, END_YEAR, PROJECT_DIR, CONDA_ENV
# ---------------------------------------------------------------------------

mkdir -p "${PROJECT_DIR}/logs"

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${CONDA_ENV}"

YEAR=${SLURM_ARRAY_TASK_ID}

echo "$(date) — dataset=${DATASET} epoch=${FUTURE_EPOCH} scenario=${SCENARIO} year=${YEAR} host=$(hostname)"

EXTRA_ARGS=""
if [ "${DATASET}" = "wbgt_future" ]; then
    EXTRA_ARGS="--future-epoch ${FUTURE_EPOCH} --scenario ${SCENARIO}"
fi

python "${PROJECT_DIR}/scripts/process_wbgt.py" \
    --year    "${YEAR}" \
    --dataset "${DATASET}" \
    --config  "${PROJECT_DIR}/config/config.yaml" \
    ${EXTRA_ARGS}

EXIT_CODE=$?
echo "$(date) — finished year ${YEAR} with exit code ${EXIT_CODE}"
exit ${EXIT_CODE}
