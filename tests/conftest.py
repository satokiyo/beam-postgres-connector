from datetime import date

import pytest

from beam_postgres.client import get_connection

HOST = "0.0.0.0"
DATABASE = "test_db"
TABLE = "test.test"
USER = "user"
PASSWORD = "passw0rd"
PORT = 5432

config = {
    "host": HOST,
    "database": DATABASE,
    "user": USER,
    "password": PASSWORD,
    "port": PORT,
}


# @pytest.fixture(scope="session", autouse=True)
# def db_init():
#    with get_connection(config) as conn:
#        cur = conn.cursor()
#
#        # prepare test data
#        try:
#            cur.execute(
#                """
#                INSERT INTO
#                    test.test(name, date, memo)
#                VALUES
#                    ('test data1', '2023-01-01', 'memo1'),
#                    ('test data2', '2023-02-02', NULL),
#                    ('test data3', '2023-03-03', 'memo3'),
#                    ('test data4', '2023-04-04', NULL),
#                    ('test data5', '2023-05-05', NULL)
#                ;
#                """
#            )
#            conn.commit()
#
#        except Exception:
#            raise "fail to initialize test db."
#
#        # execute test here.
#        yield


@pytest.fixture(scope="session")
def db_conn():
    with get_connection(config) as conn:
        # execute test here.
        yield conn


_TEST_DATA = [
    {
        "id": 1,
        "name": "test data1",
        "date": date(2023, 1, 1),
        "memo": "memo1",
    },
    {
        "id": 2,
        "name": "test data2",
        "date": date(2023, 2, 2),
        "memo": None,
    },
    {
        "id": 3,
        "name": "test data3",
        "date": date(2023, 3, 3),
        "memo": "memo3",
    },
    {
        "id": 4,
        "name": "test data4",
        "date": date(2023, 4, 4),
        "memo": None,
    },
    {
        "id": 5,
        "name": "test data5",
        "date": date(2023, 5, 5),
        "memo": None,
    },
]


@pytest.fixture()
def test_data():
    yield _TEST_DATA
