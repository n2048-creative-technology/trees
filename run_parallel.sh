#!/usr/bin/env bash
set -euo pipefail

# --- Settings ---
WORKERS=20
GLOBAL_START=1000000
GLOBAL_END=10000000       # exclusive
PYTHON_BIN=python3        # or path to your venv's python

# --- Prep ---
mkdir -p logs
echo "Starting $WORKERS workers covering [$GLOBAL_START, $GLOBAL_END)..." >&2

total=$(( GLOBAL_END - GLOBAL_START ))
chunk=$(( total / WORKERS ))
if (( chunk <= 0 )); then
  echo "Invalid chunk size. Check ranges/workers." >&2
  exit 1
fi

# --- Launch workers ---
declare -a pids=()
for i in $(seq 0 $((WORKERS - 1))); do
  start=$(( GLOBAL_START + i * chunk ))
  end=$(( start + chunk ))
  # last worker takes any remainder up to GLOBAL_END
  if (( i == WORKERS - 1 )); then
    end=$GLOBAL_END
  fi

  echo "Worker $i -> [$start, $end)" >&2

  # Launch in background; log per worker
  "$PYTHON_BIN" parser.py "$start" "$end" \
    >"logs/worker_${i}.out" 2>"logs/worker_${i}.err" &

  pids[$i]=$!
done

# --- Wait for all ---
exit_code=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    echo "Worker $i (PID ${pids[$i]}) failed. See logs/worker_${i}.err" >&2
    exit_code=1
  fi
done

if (( exit_code == 0 )); then
  echo "All workers completed successfully." >&2
else
  echo "One or more workers failed. Check logs/." >&2
fi

exit $exit_code
