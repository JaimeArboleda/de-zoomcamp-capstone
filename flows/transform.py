from pathlib import Path
from prefect import flow, task
from prefect_shell import ShellOperation

@flow()
def transform():
    ShellOperation(
        commands=[
            'docker compose up -d',
            'docker exec -d spark bash -c "spark-submit ./notebooks/transform.py"',
        ],
        working_dir=Path(__file__).parent.parent,
    ).run()

if __name__ == "__main__":
    transform()