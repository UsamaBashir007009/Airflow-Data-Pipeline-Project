from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 count_check_queries=[],
                 null_check_queroes=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.count_check_queries = count_check_queries
        self.null_check_queroes = null_check_queroes

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("--------------------Running Data Quality Checks-------------------")
        self.log.info("------Running Row Count Quality Checks---------")

        for count_check_query in self.count_check_queries:
            result = redshift.get_records(count_check_query)
            self.log.info(f"### RESULT: {result} ####")
            if len(result) < 1 or len(result[0]) < 1 or result[0][0] < 1:
                raise ValueError(f"Data quality check failed. \n{count_check_query} returned no results")

        self.log.info(f"****** Row Count Quality Check passed againsts this query: {count_check_query} ******")

        self.log.info("--------------------Running Data Quality Checks-------------------")
        self.log.info("------Running Row Count Quality Checks---------")

        for null_check_query in self.null_check_queroes:
            result = redshift.get_records(null_check_query)
            self.log.info(f"### RESULT: {result} ####")
            if len(result) > 1 or len(result[0]) > 1 or result[0][0] > 0:
                raise ValueError(
                    f"Data quality check failed. Found Null Values against the query: \n{null_check_query}")

        self.log.info(f"****** Null Count Quality Check passed againsts this query: {null_check_query} ******")
