from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 conn_id = '',
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.tests = tests
        
    def execute(self, context):
        postgres = PostgresHook(self.postgres_conn_id)
        
        for test in self.tests:
            self.log.info(f'Getting records for query: "{test.sql}"')
            test.records = postgres.get_records(test.sql)
            result = test.validate()