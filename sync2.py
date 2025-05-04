from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Укажите путь к вашему репозиторию Git
GIT_REPO_URL = "git@github.com:Mary-z/test.git"
# Укажите ветку, которую нужно отслеживать
GIT_BRANCH = "main"
# Путь, куда будет клонироваться репозиторий на воркере
LOCAL_REPO_PATH = "/opt/airflow/dags/"
# Путь, куда будут копироваться измененные DAG'и для шедулера
#DAGS_DESTINATION_PATH = "/opt/airflow/dags/dags2" # Эта директория должна быть доступна шедулеру

with DAG(
    dag_id='git_dags_sync',
    start_date=datetime(2025, 5, 4),
    schedule='@daily',
    catchup=False,
    tags=['git', 'sync'],
) as dag:
    clone_or_pull_repo = BashOperator(
        task_id='clone_or_pull_repo',
        bash_command=f"""
        if [ -d "{LOCAL_REPO_PATH}" ]; then
            rm -rf {LOCAL_REPO_PATH}/* {LOCAL_REPO_PATH}/.[!.]*
            mkdir -p {LOCAL_REPO_PATH}
        else
            mkdir -p {LOCAL_REPO_PATH}
        fi
        /usr/bin/git clone --branch {GIT_BRANCH} {GIT_REPO_URL} {LOCAL_REPO_PATH}
        """,
    )

    clone_or_pull_repo
