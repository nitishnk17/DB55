#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# run_tests.sh  —  Build and run DBMS Assignment 3 tests
# Usage: bash run_tests.sh
# ──────────────────────────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================================================"
echo "  DBMS Assignment 3 — Build + Test"
echo "  Directory: $SCRIPT_DIR"
echo "======================================================================"
echo ""

# ── 0. Regenerate monitor_config.json with correct local paths ────────────────
echo "[0/3] Regenerating test config with local paths..."
python3 "$SCRIPT_DIR/generate_tests.py"
echo ""

# ── 1. Build ──────────────────────────────────────────────────────────────────
echo "[1/3] Building all crates (release mode)..."
cargo build --release 2>&1
echo ""
echo "Build complete."
echo ""

# ── 2. Run tests via monitor ──────────────────────────────────────────────────
CONFIG="$SCRIPT_DIR/scratch/runtimes/tpch/monitor_config.json"
echo "[2/3] Running monitor with config:"
echo "      $CONFIG"
echo ""

./target/release/monitor --config "$CONFIG"

echo ""
echo "Done."
