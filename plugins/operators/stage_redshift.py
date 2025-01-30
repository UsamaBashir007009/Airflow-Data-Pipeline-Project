from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL_t = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_key="",
                 s3_bucket="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 json_path="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.json_path = "auto" if not json_path else f"s3://{s3_bucket}/{json_path}"

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Deleting data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_bkt_path = f"s3://{self.s3_bucket}/{rendered_key}/"

        copy_query = self.COPY_SQL_t.format(
            self.table,
            s3_bkt_path,
            aws_connection.login,
            aws_connection.password,
            self.json_path
        )
        redshift.run(copy_query)
        self.log.info(f"Success: Copying data from S3 to Redshift table {self.table}")
