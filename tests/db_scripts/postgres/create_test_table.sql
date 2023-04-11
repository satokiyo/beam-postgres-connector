-- DB
\c test_db

-- Schema
CREATE SCHEMA test;

CREATE TABLE IF NOT EXISTS test.test (
    id        serial,
    name      varchar(30),
    date      date NOT NULL,
    memo      varchar(50),
    PRIMARY KEY (id, date)
);

-- Records
INSERT INTO
    test.test(name, date, memo)
VALUES
    ('test data1', '2023-01-01', 'memo1'),
    ('test data2', '2023-02-02', NULL),
    ('test data3', '2023-03-03', 'memo3'),
    ('test data4', '2023-04-04', NULL),
    ('test data5', '2023-05-05', NULL)
;