"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# from random_word import RandomWords


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 19),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "test_db_update",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

    templated_command = """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.my_param }}"
        {% endfor %}
    """

    def insert_new_data():
        from db.postgres_db_access import RdsDatabase, TestData
        import time

        # rand_word = RandomWords()

        db_engine = RdsDatabase()

        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=db_engine.engine)
        session = Session()

        # create table if not existed..
        TestData.__table__.create(bind=db_engine.engine, checkfirst=True)

        # insert data
        test_data = TestData(
            date=time.time(),
            name=f"test_data_{time.time() % 100}",
        )
        session.add(test_data)
        session.commit()

    def query_all_data():

        from db.postgres_db_access import RdsDatabase, TestData
        import time

        # rand_word = RandomWords()

        db_engine = RdsDatabase()

        from sqlalchemy.orm import sessionmaker

        Session = sessionmaker(bind=db_engine.engine)
        session = Session()

        # query data
        result = session.query(TestData).all()
        for row in result:
            print(row.date, row.name)

    t2 = PythonOperator(
        task_id="new_db_data",
        python_callable=insert_new_data,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id="query_test_data",
        python_callable=query_all_data,
        dag=dag,
    )

    t4 = BashOperator(
        task_id="templated_command",
        bash_command=templated_command,
        params={"my_param": "Parameter I passed in"},
        dag=dag,
    )

    t1 >> t2 >> t3 >> t4
