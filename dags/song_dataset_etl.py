import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
    PostgresOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain

songplay_table_insert = """
        INSERT INTO songplays
        SELECT md5(events.sessionid || events.start_time) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

user_table_insert = """
        TRUNCATE TABLE users;

        INSERT INTO users
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong';
    """

artist_table_insert = """
        TRUNCATE TABLE artists;

        INSERT INTO artists
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs;
    """

song_table_insert = """
        TRUNCATE TABLE songs;

        INSERT INTO songs
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
    """

time_table_insert = """
        TRUNCATE TABLE time;

        INSERT INTO time
        SELECT start_time,
                extract(hour from start_time),
                extract(day from start_time),
                extract(week from start_time),
                extract(month from start_time),
                extract(year from start_time),
                extract(dayofweek from start_time)
        FROM songplays;
    """

S3_BUCKET = "udacity-dend"
LOG_PATH = "log_data"
SONG_PATH = "song_data/A/A/A"
REDSHIFT_CONN_ID = "redshift"
AWS_CONN_ID = "aws"

with DAG(
    "song_dataset_etl",
    description="""Load and transform data from S3 to Redshift""",
    schedule_interval="00 * * * *",
    default_args={
        "owner": "Arthur Harduim",
        "depends_on_past": False,
        "start_date": datetime(2021, 1, 1),
        "email": ["arthur@rioenergy.com.br"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
) as dag:
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        schema="public",
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_path=LOG_PATH,
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        copy_options=("REGION  'us-west-2'", f"json 's3://{S3_BUCKET}/log_json_path.json'"),
        autocommit=True,
    )

    stage_songs_to_redshift = DummyOperator(task_id="Stage_songs")
    # stage_songs_to_redshift = StageToRedshiftOperator(
    #     task_id="Stage_songs",
    #     schema="public",
    #     table="staging_songs",
    #     s3_bucket=S3_BUCKET,
    #     s3_path=SONG_PATH,
    #     redshift_conn_id=REDSHIFT_CONN_ID,
    #     aws_conn_id=AWS_CONN_ID,
    #     copy_options=("REGION 'us-west-2'", "json 'auto'"),
    #     autocommit=True,
    # )

    load_songplays_table = PostgresOperator(
        task_id="Load_songplays_fact_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=songplay_table_insert,
        autocommit=True,
    )
    load_user_dimension_table = PostgresOperator(
        task_id="Load_user_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=user_table_insert,
        autocommit=True,
    )
    load_song_dimension_table = PostgresOperator(
        task_id="Load_song_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=song_table_insert,
        autocommit=True,
    )
    load_artist_dimension_table = PostgresOperator(
        task_id="Load_artist_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=artist_table_insert,
        autocommit=True,
    )
    load_time_dimension_table = PostgresOperator(
        task_id="Load_time_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=time_table_insert,
        autocommit=True,
    )
    run_quality_checks = DummyOperator(task_id="Run_data_quality_checks")
    chain(
        [stage_events_to_redshift, stage_songs_to_redshift],
        load_songplays_table,
        [
            load_user_dimension_table,
            load_song_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ],
        run_quality_checks,
    )
