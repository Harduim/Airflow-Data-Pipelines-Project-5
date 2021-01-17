from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = "postgres_default",
        truncate: str = "",
        *args,
        **kwargs,
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.truncate:
            trunc_query = f"TRUNCATE TABLE {self.truncate};"
            self.log.info("Executing: \n%s", trunc_query)
            self.hook.run(trunc_query, True)

        self.hook.run(self.sql, True)
        for output in self.hook.conn.notices:
            self.log.info(output)
