CREATE TABLE magic_squares.candidate_rows_columns_and_diagonal WITH (
  format = 'parquet',
  external_location = 's3://m4.squares.megadodo.umb/candidate-rows-columns-and-diagonal/',
  write_compression = 'SNAPPY'
) AS
SELECT 
    row_number() OVER () AS id,
    crc.candidate_rows,
    crc.repvalues_candidate_rows,
    crc.candidate_columns,
    crc.repvalues_candidate_columns,
    p.row AS diagonal_row,
    p.repValue AS diagonal_repvalue
FROM 
    magic_squares.candidate_rows_and_columns crc
CROSS JOIN 
    magic_squares.partitions p
WHERE 
    NOT contains(crc.repvalues_candidate_rows, p.repValue)
    AND NOT contains(crc.repvalues_candidate_columns, p.repValue)
    and cardinality(array_intersect(crc.candidate_rows[1], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_rows[2], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_rows[3], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_rows[4], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_columns[1], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_columns[2], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_columns[3], p.row)) = 1
    and cardinality(array_intersect(crc.candidate_columns[4], p.row)) = 1