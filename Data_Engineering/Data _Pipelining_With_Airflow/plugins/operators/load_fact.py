from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_fact = '''
    INSERT INTO {}(
        playid,
        start_time,
        userid,
        level,
        songid, 
        artistid, 
        sessionid, 
        location, 
        user_agent
    )
    {}
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="songplays",
                 sql=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.sql = sql


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f'inserting data into {self.table} table')
        formatted_sql = LoadFactOperator.insert_fact.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
