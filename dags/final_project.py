from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers.data_quality_queries import (data_quality_count_check_queries, 
                                          data_quality_null_check_queries)

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_key="log-data",
        s3_bucket="usama-airflow",
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        json_path="log_json_path.json",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_key="song-data",
        s3_bucket="usama-airflow",
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        json_path="",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="songplays",
        redshift_conn_id="redshift",
        query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        redshift_conn_id="redshift",
        query=SqlQueries.user_table_insert,
        mode="truncate-insert"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        redshift_conn_id="redshift",
        query=SqlQueries.song_table_insert,
        mode="truncate-insert"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        redshift_conn_id="redshift",
        query=SqlQueries.artist_table_insert,
        mode="truncate-insert"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        redshift_conn_id="redshift",
        query=SqlQueries.time_table_insert,
        mode="truncate-insert"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        count_check_queries=data_quality_count_check_queries,
        null_check_queroes=data_quality_null_check_queries
    )

    end_operator = DummyOperator(task_id='end_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table,
                             load_song_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
