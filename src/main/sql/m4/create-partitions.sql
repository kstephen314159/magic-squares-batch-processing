CREATE EXTERNAL TABLE IF NOT EXISTS magic_squares.partitions (
    row ARRAY<INT>,
    repValue INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://m4.squares.megadodo.umb/partitions/'
TBLPROPERTIES (
    'has_encrypted_data'='false'
);
