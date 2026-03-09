#!/bin/bash
# ---------------------------------------------------------------------------
# Submit all WBGT processing jobs to SLURM.
#
# Submits one job array per dataset/epoch/scenario combination:
#   1. wbgt           (CHIRTS-ERA5, 1980–2025)
#   2. wbgt_baseline  (CHC-CMIP6 observed WBGTmax, 1983–2016)
#   3. wbgt_future    2030 × ssp245  (1983–2016)
#   4. wbgt_future    2030 × ssp585  (1983–2016)
#   5. wbgt_future    2050 × ssp245  (1983–2016)
#   6. wbgt_future    2050 × ssp585  (1983–2016)
#
# Usage:
#   bash submit_all.sh
# ---------------------------------------------------------------------------

# === PATHS ============================================================
PROJECT_DIR="/soge-home/projects/Jadapt/howden_heat"   # absolute path on the cluster
CONDA_ENV="howden_heat"
SLURM_SCRIPT="${PROJECT_DIR}/cluster/slurm_wbgt.sh"
# ==========================================================================

mkdir -p "${PROJECT_DIR}/logs"

BASE_EXPORT="PROJECT_DIR=${PROJECT_DIR},CONDA_ENV=${CONDA_ENV}"

# ---------------------------------------------------------------------------
# 1. CHIRTS-ERA5 WBGT (original)
# ---------------------------------------------------------------------------
# Array indices are 0-based; year = START_YEAR + SLURM_ARRAY_TASK_ID
sbatch \
  --job-name=wbgt \
  --array=0-45 \
  --export="${BASE_EXPORT},DATASET=wbgt,FUTURE_EPOCH=none,SCENARIO=none,START_YEAR=1980" \
  "${SLURM_SCRIPT}"
echo "Submitted: wbgt 1980-2025 (indices 0-45)"

# ---------------------------------------------------------------------------
# 2. CHC-CMIP6 WBGTmax baseline
# ---------------------------------------------------------------------------
sbatch \
  --job-name=wbgt_baseline \
  --array=0-33 \
  --export="${BASE_EXPORT},DATASET=wbgt_baseline,FUTURE_EPOCH=none,SCENARIO=none,START_YEAR=1983" \
  "${SLURM_SCRIPT}"
echo "Submitted: wbgt_baseline 1983-2016 (indices 0-33)"

# ---------------------------------------------------------------------------
# 3-6. CHC-CMIP6 WBGTmax future (2 epochs × 2 scenarios)
# ---------------------------------------------------------------------------
for EPOCH in 2030 2050; do
  for SCENARIO in ssp245 ssp585; do
    JOB_NAME="wbgt_future_${EPOCH}_${SCENARIO}"
    sbatch \
      --job-name="${JOB_NAME}" \
      --array=0-33 \
      --export="${BASE_EXPORT},DATASET=wbgt_future,FUTURE_EPOCH=${EPOCH},SCENARIO=${SCENARIO},START_YEAR=1983" \
      "${SLURM_SCRIPT}"
    echo "Submitted: ${JOB_NAME} 1983-2016 (indices 0-33)"
  done
done

echo ""
echo "All jobs submitted. Monitor with: squeue -u \$USER"
echo "Logs will appear in: ${PROJECT_DIR}/logs/"
