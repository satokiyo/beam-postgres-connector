from typing import Dict, Union

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.options.value_provider import ValueProvider
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import PTransform

from beam_postgres import splitters
from beam_postgres.client import PostgresClient
from beam_postgres.source import PostgresSource
from beam_postgres.utils import get_runtime_value


class ReadFromPostgres(PTransform):
    """Create PCollection from PostgreSQL."""

    def __init__(
        self,
        query: Union[str, ValueProvider],
        host: Union[str, ValueProvider],
        database: Union[str, ValueProvider],
        user: Union[str, ValueProvider],
        password: Union[str, ValueProvider],
        port: Union[int, ValueProvider] = 5432,
        splitter=splitters.NoSplitter(),
    ):
        super().__init__()
        self._query = query
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._splitter = splitter

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | iobase.Read(
            PostgresSource(
                self._query,
                self._host,
                self._database,
                self._user,
                self._password,
                self._port,
                self._splitter,
            )
        )


class WriteToPostgres(PTransform):
    """Write dict rows to Postgres."""

    def __init__(
        self,
        host: Union[str, ValueProvider],
        database: Union[str, ValueProvider],
        table: Union[str, ValueProvider],
        user: Union[str, ValueProvider],
        password: Union[str, ValueProvider],
        port: Union[int, ValueProvider] = 5432,
        batch_size: int = 1000,
    ):
        super().__init__()
        self._host = host
        self._database = database
        self._table = table
        self._user = user
        self._password = password
        self._port = port
        self._batch_size = batch_size

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | beam.ParDo(
            _WriteToPostgresFn(
                self._host,
                self._database,
                self._table,
                self._user,
                self._password,
                self._port,
                self._batch_size,
            )
        )


class _WriteToPostgresFn(beam.DoFn):
    """DoFn for WriteToPostgres."""

    def __init__(
        self,
        host: Union[str, ValueProvider],
        database: Union[str, ValueProvider],
        table: Union[str, ValueProvider],
        user: Union[str, ValueProvider],
        password: Union[str, ValueProvider],
        port: Union[int, ValueProvider],
        batch_size: int,
    ):
        super().__init__()
        self._host = host
        self._database = database
        self._table = table
        self._user = user
        self._password = password
        self._port = port
        self._batch_size = batch_size

        self._config = {
            "host": self._host,
            "database": self._database,
            "user": self._user,
            "password": self._password,
            "port": self._port,
        }

    def start_bundle(self):
        self._build_value()
        self._queries = []

    def process(self, element: Dict, *args, **kwargs):
        columns = []
        values = []
        for column, value in element.items():
            columns.append(column)
            values.append(value)

        column_str = ", ".join(columns)
        value_str = ", ".join(
            [
                f"{'NULL' if value is None else value}"
                if isinstance(value, (type(None), int, float))
                else f"'{value}'"
                for value in values
            ]
        )

        query = f"INSERT INTO {self._config['database']}.{self._table}({column_str}) VALUES({value_str});"

        self._queries.append(query)

        if len(self._queries) > self._batch_size:
            self._client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def finish_bundle(self):
        if len(self._queries):
            self._client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def _build_value(self):
        for k, v in self._config.items():
            self._config[k] = get_runtime_value(v)
        self._table = get_runtime_value(self._table)
        self._batch_size = get_runtime_value(self._batch_size)

        self._client = PostgresClient(self._config)
