from contextlib import contextmanager
from logging import INFO, getLogger
import re
from typing import Dict, Generator, List

from psycopg2 import Error as PostgresConnectorError
import psycopg2.extras

from beam_postgres.errors import BeamPostgresClientError

_SELECT_STATEMENT = "SELECT"
_INSERT_STATEMENT = "INSERT"

logger = getLogger(__name__)
logger.setLevel(INFO)


class PostgresClient:
    """A postgres client object."""

    def __init__(self, config: Dict):
        self._config = config
        self._validate_config(self._config)

    def record_generator(self, query: str) -> Generator[Dict, None, None]:
        """
        Generate dict record from raw data on postgres.

        Args:
            query: query with select statement

        Returns:
            dict record

        Raises:
            ~beam_postgres.errors.BeamPostgresClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])

        with get_connection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            try:
                cur.execute(query)
                logger.info(f"Successfully execute query: {query}")

                for record in cur:
                    yield dict(record)
            except PostgresConnectorError as e:
                raise BeamPostgresClientError(
                    "Failed to execute query: %s, Raise exception: %s", query, e
                )

            cur.close()

    def rough_counts_estimator(self, query: str) -> int:
        """
        Make a rough estimate of the total number of records.
        To avoid waiting time by select counts query when the data size is too large.

        Args:
            query: query with select statement

        Returns:
            the total number of records

        Raises:
            ~beam_postgres.errors.BeamPostgresClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])
        count_query = f"EXPLAIN SELECT * FROM ({query}) as subq"

        with get_connection(self._config) as conn:
            cur = conn.cursor()

            try:
                cur.execute(count_query)
                logger.info(f"Successfully execute query: {count_query}")

                records = cur.fetchall()
                res_str = records[0][0]

                total_number = 0
                matched = re.search("rows=([0-9]+)", res_str)
                if matched is not None:
                    total_number = int(matched.group(1))

            except PostgresConnectorError as e:
                raise BeamPostgresClientError(
                    "Failed to execute query: %s, Raise exception: %s", count_query, e
                )

            cur.close()

            if total_number <= 0:
                raise BeamPostgresClientError(
                    "Failed to estimate total number of records. Query: %s", count_query
                )
            else:
                return total_number

    def record_loader(self, query: str) -> None:
        """
        Load dict record into postgres.

        Args:
            query: query with insert or update statement

        Raises:
            ~beam_postgres.errors.BeamPostgresClientError
        """
        self._validate_query(query, [_INSERT_STATEMENT])

        with get_connection(self._config) as conn:
            cur = conn.cursor()

            try:
                cur.execute(query)
                conn.commit()
                logger.info(f"Successfully execute query: {query}")
            except PostgresConnectorError as e:
                conn.rollback()
                raise BeamPostgresClientError(
                    "Failed to execute query: %s, Raise exception: %s", query, e
                )

            cur.close()

    @staticmethod
    def _validate_config(config: Dict) -> None:
        required_keys = ["host", "port", "database", "user", "password"]
        for required in required_keys:
            if required not in config.keys():
                raise BeamPostgresClientError(
                    "Config is not satisfied. required: %s", required
                )

    @staticmethod
    def _validate_query(query: str, statements: List[str]) -> None:
        query = query.lstrip()

        for statement in statements:
            if statement and not query.lower().startswith(statement.lower()):
                raise BeamPostgresClientError(
                    "Query expected to start with %s statement. Query: %s",
                    statement,
                    query,
                )


@contextmanager
def get_connection(config: Dict):
    """A wrapper object to connect postgres."""
    try:
        conn = psycopg2.connect(**config)
        yield conn

    except PostgresConnectorError as e:
        raise PostgresConnectorError(
            f"Failed to connect postgres, Raise exception: {e}"
        )
    finally:
        conn.close()
