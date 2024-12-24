from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Instantiate your DAG
@dag(dag_id="Aloha_Please_Wait_For_Dags_To_Populate",schedule=None,catchup=False)
def startproducingtotopic():
   def empty():
     pass
