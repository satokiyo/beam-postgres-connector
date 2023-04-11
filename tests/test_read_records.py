from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_postgres import splitters
from beam_postgres.io import ReadFromPostgres

HOST = "0.0.0.0"
DATABASE = "test_db"
USER = "user"
PASSWORD = "passw0rd"
PORT = 5432


class TestReadRecordsPipeline:
    def test_pipeline_no_splitter(self, test_data):
        expected = test_data[:]

        with TestPipeline() as p:
            # Access to postgres on docker
            read_from_postgres = ReadFromPostgres(
                query="SELECT * FROM test_db.test.test;",
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                port=PORT,
                splitter=splitters.NoSplitter(),
            )

            actual = p | read_from_postgres

            assert_that(actual, equal_to(expected))

    def test_pipeline_query_splitter(self, test_data):
        expected = test_data[:3]
        with TestPipeline() as p:
            # Access to postgres on docker
            read_from_postgres = ReadFromPostgres(
                query="SELECT * FROM test_db.test.test where id = 1; --sep-- SELECT * FROM test_db.test.test where id = 2; --sep-- SELECT * FROM test_db.test.test where id = 3;",
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                port=PORT,
                splitter=splitters.QuerySplitter(sep="--sep--"),
            )

            actual = p | read_from_postgres

            assert_that(actual, equal_to(expected))
