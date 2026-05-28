from prefect import task
from mlflow.tracking import MlflowClient


@task(
    name="ensure-experiment-active",
    description="Ensure that an MLflow experiment is active (not deleted)",
    tags=["mlflow", "experiment"],
)
def ensure_experiment_active(experiment_name: str) -> None:
    """
    Ensure that an MLflow experiment is active (not deleted).

    If the experiment with the given name exists and is in the "deleted" lifecycle stage,
    this function restores it to the "active" state. If the experiment does not exist or
    is already active, no action is taken.

    Parameters
    ----------
    experiment_name : str
        The name of the MLflow experiment to check and restore if necessary.

    Returns
    -------
    None
        This function does not return a value. It performs the restoration as a side effect.

    Notes
    -----
    - Uses the MLflowClient to interact with the MLflow tracking server.
    - This function is decorated as a Prefect task for orchestration in data pipelines.
    """
    client = MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp and exp.lifecycle_stage == "deleted":
        client.restore_experiment(exp.experiment_id)
