import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from beam_postgres.io import WriteToPostgres

HOST = "0.0.0.0"
DATABASE = "test_db"
TABLE = "test.test"
USER = "user"
PASSWORD = "passw0rd"
PORT = 5432
BATCH_SIZE = 0


class TestWriteRecordsPipeline:
    def test_pipeline(self, db_conn, test_data):
        test_data = test_data[:]
        insert_data = {
            "id": 6,
            "name": "test data6",
            "date": datetime.date(2023, 6, 6),
            "memo": None,
        }
        test_data.append(insert_data)

        expected = [tuple(dict_item.values()) for dict_item in test_data]

        with TestPipeline() as p:
            # Access to postgres on docker
            write_to_postgres = WriteToPostgres(
                host=HOST,
                database=DATABASE,
                table=TABLE,
                user=USER,
                password=PASSWORD,
                port=PORT,
                batch_size=BATCH_SIZE,
            )

            p | beam.Create([insert_data]) | write_to_postgres

        with db_conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {DATABASE}.{TABLE}")
            actual = cur.fetchall()

        assert actual == expected

        # tear down
        with db_conn.cursor() as cur:
            cur.execute(f"DELETE FROM {DATABASE}.{TABLE} WHERE id = 6;")
            db_conn.commit()
