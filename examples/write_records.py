import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_postgres.io import WriteToPostgres


class WriteRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--host", dest="host", default="0.0.0.0")
        parser.add_value_provider_argument("--port", dest="port", default=5432)
        parser.add_value_provider_argument(
            "--database", dest="database", default="test_db"
        )
        parser.add_value_provider_argument("--table", dest="table", default="test.test")
        parser.add_value_provider_argument("--user", dest="user", default="user")
        parser.add_value_provider_argument(
            "--password", dest="password", default="passw0rd"
        )
        parser.add_value_provider_argument("--batch_size", dest="batch_size", default=0)


def run():
    options = WriteRecordsOptions()

    with beam.Pipeline(options=options) as p:
        write_to_postgres = WriteToPostgres(
            host=options.host,
            database=options.database,
            table=options.table,
            user=options.user,
            password=options.password,
            port=options.port,
            batch_size=options.batch_size,
        )

        (
            p
            | "ReadFromInMemory"
            >> beam.Create(
                [
                    {
                        "id": 6,
                        "name": "test data6",
                        "date": datetime.date(2023, 6, 6),
                        "memo": None,
                    }
                ]
            )
            | "WriteToPostgres" >> write_to_postgres
        )


if __name__ == "__main__":
    run()
