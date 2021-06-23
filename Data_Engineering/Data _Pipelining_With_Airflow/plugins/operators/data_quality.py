from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 checks = [],
                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        task_instance = context['task_instance']
        table = task_instance.xcom_pull(key='table_name')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        for check in self.checks:
            records = redshift.get_records(check.check_sql.format(table))
            if len(records) < check.expected_result or len(records[0]) <  check.expected_result:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < check.expected_result:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")