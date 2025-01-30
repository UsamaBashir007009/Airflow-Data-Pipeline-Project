from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 mode="",
                 table="",
                 query="",
                 redshift_conn_id="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == "truncate-insert":
            self.log.info(f"Deleting data from table: {self.table}")
            truncate_query = f"TRUNCATE TABLE {self.table};"
            redshift.run(truncate_query)
            self.log.info(f"Successfully deleted data from table: {self.table}")

        self.log.info("Loading data from staging to fact table")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info(f"Successfully inserted data from staging to fact table: {self.table}")
