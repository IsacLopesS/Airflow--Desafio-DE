from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3
from pandasql import sqldf

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##
def join_tables():
    conn = sqlite3.connect("/home/isac/Documentos/desafioDE/airflow_tooltorial/data/Northwind_small.sqlite")
    df_OrderDetail = pd.read_sql_query("SELECT * from 'OrderDetail';",conn)
    df_Order = pd.read_csv("output_orders.csv")
    df_join = pd.merge(df_Order,df_OrderDetail,left_on="Id", right_on="OrderId")
    consulta = '''select SUM(Quantity) total
                    from df_join
                    where ShipCity = "Rio de Janeiro"
                '''
    resultado = sqldf(consulta)
    with open('/home/isac/Documentos/desafioDE/airflow_tooltorial/count.txt', 'w') as arquivo:
        arquivo.write(str(resultado.total.iloc[0]))
    
def extract_Orders_table():
    conn = sqlite3.connect("/home/isac/Documentos/desafioDE/airflow_tooltorial/data/Northwind_small.sqlite")
    df = pd.read_sql_query("SELECT * from 'Order';",conn)
    df.to_csv('/home/isac/Documentos/desafioDE/airflow_tooltorial/output_orders.csv', index=False)



with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    extract_Orders_table = PythonOperator(
        task_id="extract_Orders_table",
        python_callable=extract_Orders_table,
        provide_context = True         
    )

    join_tables = PythonOperator(
        task_id="join_tables",
        python_callable=join_tables,
        provide_context = True
    )

    extract_Orders_table >> join_tables >> export_final_output
   