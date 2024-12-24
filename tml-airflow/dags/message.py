from airflow import DAG
from airflow.decorators import dag, task

# Instantiate your DAG
@dag(dag_id="Aloha_Please_Wait_For_Dags_To_Populate",default_args={}, tags=[""],schedule=None,catchup=False)
def message():
   def empty():
     pass
dag = message()
