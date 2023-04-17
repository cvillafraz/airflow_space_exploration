from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator


def _generate_platzi_data(**kwargs):
    import pandas as pd

    data = pd.DataFrame(
        {
            "student": [
                "Maria Cruz",
                "Daniel Crema",
                "Elon Musk",
                "Karol Castrejon",
                "Freddy Vega",
            ],
            "timestamp": [
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
            ],
        }
    )
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv", header=True)


@dag(
    description="Data extract",
    start_date=datetime(2020, 10, 2),
    schedule_interval="@once",
)
def data_extract():
    task_nasa = BashOperator(
        task_id="nasa_confirm",
        bash_command='sleep 10 && echo "OK" > /tmp/response_{{ds_nodash}}.txt',
    )

    nasa_sensor = FileSensor(
        task_id="wait_for_nasa_permission", filepath="/tmp/response_{{ds_nodash}}.txt"
    )

    spacex_curl = BashOperator(
        task_id="curl_spacex_data",
        bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'",
    )

    @task
    def generate_satelite_data(**kwargs):
        _generate_platzi_data(**kwargs)

    cat_satelite_data = BashOperator(
        task_id="cat_satelite_data",
        bash_command="ls /tmp && head /tmp/platzi_data_{{ds_nodash}}.csv",
    )

    notify_team = EmailOperator(
        task_id="notify_team",
        to="cirov@protonmail.com",
        subject="Data available",
        html_content="Data retrieved on {{ds}} is available",
        conn_id="gmail",
    )

    gen_satelite_data = generate_satelite_data()
    task_nasa >> nasa_sensor >> gen_satelite_data >> notify_team
    gen_satelite_data >> cat_satelite_data
    spacex_curl >> notify_team


data_extract()
