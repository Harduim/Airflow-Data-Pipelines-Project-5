from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Tuple


class StageToRedshiftOperator(BaseOperator):
    "Based on https://github.com/apache/airflow/blob/v1-10-stable/airflow/operators/s3_to_redshift_operator.py"

    template_fields = ()
    template_ext = ()
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_path: str,
        redshift_conn_id: str = "redshift_default",
        aws_conn_id: str = "aws_default",
        copy_options: Tuple[str] = tuple(),
        autocommit: bool = True,
        *args,
        **kwargs,
    ):
        """Executes a COPY command to load files from s3 to Redshift

        Args:
            schema (str): schema in redshift database
            table (str):  table in redshift database
            s3_bucket (str): S3 bucket
            s3_path (str): S3 path to files
            redshift_conn_id (str, optional): Redshift connection id. Defaults to 'redshift_default'.
            aws_conn_id (str, optional): AWS connection id. Defaults to 'aws_default'.
            copy_options (tuple, optional): List of COPY options. Defaults to tuple().
            autocommit (bool, optional): Defaults to True.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self.autocommit = autocommit

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()
        copy_options = "\n\t\t".join(self.copy_options)

        copy_query = f"""
            COPY {self.schema}.{self.table}
            FROM 's3://{self.s3_bucket}/{self.s3_path}'
            with credentials
            'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
            {copy_options};
        """
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY complete")
