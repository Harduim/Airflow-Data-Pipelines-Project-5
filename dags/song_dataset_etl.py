from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain

songplay_table_insert = """INSERT INTO songplays
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
                AND events.length = songs.duration;"""

user_table_insert = """INSERT INTO users
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong';"""

artist_table_insert = """INSERT INTO artists
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs; """

song_table_insert = """INSERT INTO songs
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;"""

time_table_insert = """INSERT INTO time
        SELECT start_time,
                extract(hour from start_time),
                extract(day from start_time),
                extract(week from start_time),
                extract(month from start_time),
                extract(year from start_time),
                extract(dayofweek from start_time)
        FROM songplays;"""


def result_not_zero(result: list):
    """ "Returns True if query result is bigger than 0"

    Args:
        result (list): Result from a sqlalchemy query

    Returns:
        bool: True of False
    """
    return result[0][0] > 0


S3_BUCKET = "udacity-dend"
LOG_PATH = "log_data"
SONG_PATH = "song_data/A/A/A"
REDSHIFT_CONN_ID = "redshift"
AWS_CONN_ID = "aws"

with DAG(
    "song_dataset_etl",
    description="""Load and transform data from S3 to Redshift""",
    schedule_interval="@hourly",
    default_args={
        "owner": "Arthur Harduim",
        "depends_on_past": False,
        "start_date": datetime(2021, 1, 1),
        "email": ["arthur@rioenergy.com.br"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "catchup": False
    },
) as dag:
    start_operator = DummyOperator(task_id="Begin_execution")

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
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        schema="public",
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_path=SONG_PATH,
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        copy_options=("REGION 'us-west-2'", "json 'auto'"),
        autocommit=True,
    )
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=songplay_table_insert,
        autocommit=True,
    )
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=user_table_insert,
        truncate="users",
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=song_table_insert,
        truncate="songs",
    )
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=artist_table_insert,
        truncate="artists",
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=time_table_insert,
        truncate="time",
    )
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        conn_id=REDSHIFT_CONN_ID,
        test_cases=(
            ("SELECT COUNT(playid) FROM songplays", result_not_zero),
            ("SELECT COUNT(userid) FROM users", result_not_zero),
            ("SELECT COUNT(artistid) FROM artists", result_not_zero),
            ("SELECT COUNT(songid) FROM songs", result_not_zero),
            ("SELECT COUNT(start_time) FROM time", result_not_zero),
            (
                "SELECT (SELECT COUNT(start_time) FROM songplays) - (SELECT COUNT(start_time) FROM time)",
                lambda result: result[0][0] == 0,
            ),
            ("SELECT COUNT(userid) FROM users WHERE first_name IS NOT NULL", result_not_zero),
            ("SELECT COUNT(userid) FROM users WHERE last_name IS NOT NULL", result_not_zero),
            ("SELECT COUNT(DISTINCT(week)) FROM time", lambda result: result[0][0] <= 46),
            ("SELECT COUNT(DISTINCT(hour)) FROM time", lambda result: result[0][0] <= 24),
            ("SELECT COUNT(DISTINCT(month)) FROM time", lambda result: result[0][0] <= 12),
        ),
    )
    end_operator = DummyOperator(task_id="Stop_execution")
    chain(
        start_operator,
        [stage_events_to_redshift, stage_songs_to_redshift],
        load_songplays_table,
        [
            load_user_dimension_table,
            load_song_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ],
        run_quality_checks,
        end_operator,
    )
