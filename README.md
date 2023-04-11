# Beam - Postgres connector

[![PyPI](https://img.shields.io/pypi/v/beam-postgres-connector.svg)][pypi-project]
[![Supported Versions](https://img.shields.io/pypi/pyversions/beam-postgres-connector.svg)][pypi-project]
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Beam - Postgres Connector provides an io connector for PostgreSQL read/write in [Apache Beam](https://beam.apache.org/) pipelines.

## Installation

```bash
pip install beam-postgres-connector
```

## Usage

- Read From PostgreSQL

```Python
import apache_beam as beam
from beam_postgres import splitters
from beam_postgres.io import ReadFromPostgres

with beam.Pipeline(options=options) as p:
    read_from_postgres = ReadFromPostgres(
            query="SELECT * FROM test_db.test.test;",
            host="localhost",
            database="test_db",
            user="test",
            password="test",
            port=5432,
            splitter=splitters.NoSplitter()  # you can select how to split query for performance
    )


    (
        p
        | "ReadFromPostgres" >> read_from_postgres
        | "WriteToStdout" >> beam.Map(print)
    )
```

- Write To MySQL

```Python
import apache_beam as beam
from beam_postgres.io import WriteToPostgres


with beam.Pipeline(options=options) as p:
    write_to_postgres = WriteToPostgres(
            host="localhost",
            database="test_db",
            table="test.test",
            user="test",
            password="test",
            port=5432,
            batch_size=1000,
    )

    (
        p
        | "ReadFromInMemory"
        >> beam.Create(
            [
                {
                    "name": "test data",
                }
            ]
        )
        | "WriteToPostgres" >> write_to_postgres
    )
```

See [here][examples] for more examples.

## splitters

- NoSplitter

  Do not split the query

- QuerySplitter

  Split the query by a specified separator string and distribute it for parallel processing across multiple nodes. Specify non-overlapping ranges for each query in the WHERE clause. The processing results will be combined using UNION ALL.

## License

MIT License.

[pypi-project]: https://pypi.org/project/beam-postgres-connector
[examples]: https://github.com/satokiyo/beam-postgres-connector/tree/main/examples
