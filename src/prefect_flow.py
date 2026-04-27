from prefect import flow, task
import subprocess
import os

# =========================
# CONFIG
# =========================
BASE_DIR = "/app"   # IMPORTANT: Docker path

# =========================
# TASKS
# =========================
@task
def run_ingestion():
    print("Running ingestion...")
    subprocess.run(["python", "src/ingestion.py"], check=True)


@task
def run_processing():
    print("Running processing...")
    subprocess.run(["python", "src/process.py"], check=True)


# =========================
# FLOW
# =========================
@flow(name="crypto-pipeline")
def crypto_pipeline():
    os.chdir(BASE_DIR)   # CRITICAL FIX
    run_ingestion()
    run_processing()


if __name__ == "__main__":
    crypto_pipeline()