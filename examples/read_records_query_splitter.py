import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_postgres import splitters
from beam_postgres.io import ReadFromPostgres


class ReadRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--host", dest="host", default="localhost")
        parser.add_value_provider_argument("--port", dest="port", default=5432)
        parser.add_value_provider_argument(
            "--database", dest="database", default="test_db"
        )
        parser.add_value_provider_argument(
            "--query",
            dest="query",
            default="SELECT * FROM test_db.test.test WHERE id <= 3; --sep-- SELECT * FROM test_db.test.test WHERE id > 3;",
        )
        parser.add_value_provider_argument("--user", dest="user", default="user")
        parser.add_value_provider_argument(
            "--password", dest="password", default="passw0rd"
        )


def run():
    options = ReadRecordsOptions()

    with beam.Pipeline(options=options) as p:
        read_from_postgres = ReadFromPostgres(
            query=options.query,
            host=options.host,
            database=options.database,
            user=options.user,
            password=options.password,
            port=options.port,
            splitter=splitters.QuerySplitter(sep="--sep--"),
        )

        (
            p
            | "ReadFromPostgres" >> read_from_postgres
            | "WriteToStdout" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
