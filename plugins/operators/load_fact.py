from airflow.operators import PostgresOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(PostgresOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)