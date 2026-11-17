"""
switch_envoy.py — Easily toggle Envoy configuration across all Assignment 3 parts.

Supported modes:
    py -3 switch_envoy.py baseline      # Part B: Replica A only (100% on A, B & C idle)
    py -3 switch_envoy.py balanced      # Part C: Round Robin A, B, C (33% : 33% : 33%)
    py -3 switch_envoy.py weighted      # Part D Exp 1: Weighted Round Robin (2:1:1 -> 50% : 25% : 25%)
    py -3 switch_envoy.py least-req     # Part D Exp 2: Least Request load balancing
    py -3 switch_envoy.py healthcheck   # Part D Exp 3: Active Health Checking with auto-recovery
"""

import sys
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENVOY_DIR = ROOT / "envoy"
TARGET_FILE = ENVOY_DIR / "envoy.yaml"

CONFIG_MAP = {
    "baseline": ENVOY_DIR / "envoy-part-b-baseline.yaml",
    "balanced": ENVOY_DIR / "envoy-part-c-balanced.yaml",
    "weighted": ENVOY_DIR / "envoy-part-d-weighted.yaml",
    "least-req": ENVOY_DIR / "envoy-part-d-least-req.yaml",
    "healthcheck": ENVOY_DIR / "envoy-part-d-healthcheck.yaml",
}


def main():
    if len(sys.argv) < 2 or sys.argv[1].lower() not in CONFIG_MAP:
        print("Usage: py -3 switch_envoy.py <mode>")
        print("Available modes:")
        for mode, path in CONFIG_MAP.items():
            print(f"  {mode:<12} -> {path.name}")
        sys.exit(1)

    mode = sys.argv[1].lower()
    source_file = CONFIG_MAP[mode]

    if not source_file.exists():
        print(f"Error: {source_file} not found.")
        sys.exit(1)

    shutil.copyfile(source_file, TARGET_FILE)
    print(f"[OK] Applied: {source_file.name} -> envoy.yaml (Mode: {mode.upper()})")

    # Restart envoy container
    print("Restarting Envoy container...")
    try:
        res = subprocess.run(["docker", "compose", "restart", "envoy"], capture_output=True, text=True)
        if res.returncode == 0:
            print("[OK] Envoy restarted successfully!")
        else:
            print(f"[Notice] Docker restart message: {res.stderr.strip() or res.stdout.strip()}")
            print("To ensure restart, run: docker compose restart envoy")
    except Exception as e:
        print(f"[Notice] Run manually in terminal: docker compose restart envoy ({e})")


if __name__ == "__main__":
    main()
