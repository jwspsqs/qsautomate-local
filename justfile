set dotenv-load
set positional-arguments
set shell := ["bash", "-c"]


default:
    @just --list

[group('setup')]
@mlflow-start port='8031':
    mlflow server --port {{port}} --backend-store-uri ~/.qsresearch/mlflow/runs --default-artifact-root ~/.qsresearch/mlflow/artifacts

[group('setup')]
@prefect-start:
    prefect server start
