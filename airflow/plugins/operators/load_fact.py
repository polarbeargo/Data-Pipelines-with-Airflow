from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 postgres_conn_id="",
                 table="",
                 sql="",
                 if_exists="append",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.if_exists:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load fact table {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
