import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.decorators import dag

import sql_create_table_queries


@dag(
    start_date=pendulum.now(),
     max_active_runs=1
)
def create_tables():
    create_staging_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.staging_events_table_create
    )

    create_staging_songs_table = PostgresOperator(
        task_id='create_staging_songs_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.staging_songs_table_create
    )

    create_artists_table = PostgresOperator(
        task_id='create_artists_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.artist_table_create
    )

    create_songplays_table = PostgresOperator(
        task_id='create_songplays_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.songplay_table_create
    )

    create_songs_table = PostgresOperator(
        task_id='create_songs_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.song_table_create
    )

    create_time_table = PostgresOperator(
        task_id='create_time_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.time_table_create
    )

    create_users_table = PostgresOperator(
        task_id='create_users_table',
        postgres_conn_id='redshift',
        sql=sql_create_table_queries.user_table_create
    )


create_tables_dag = create_tables()
