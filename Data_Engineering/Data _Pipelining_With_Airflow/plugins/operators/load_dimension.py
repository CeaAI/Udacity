from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_dimensions = '''
    INSERT INTO {}
    ({})
    {}
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 columns ="",
                 sql=None,
                 operation ="truncate",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.colums = columns
        self.sql = sql
        self.operation = operation

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.operation == "truncate":
            self.log.info('Clear Redshift table')
            redshift.run("DELETE FROM {}".format(self.table))
   
        self.log.info(f'inserting data into {self.table} table')
        formatted_sql = LoadDimensionOperator.insert_dimensions.format(
            self.table,
            self.colums,
            self.sql
        )
        redshift.run(formatted_sql)
        task_instance = context['task_instance']
        task_instance.xcom_push(key='table_name', value=self.table)
        

