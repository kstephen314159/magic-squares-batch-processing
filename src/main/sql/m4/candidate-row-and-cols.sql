CREATE TABLE magic_squares.candidate_rows_and_columns WITH (
  format = 'parquet',
  external_location = 's3://m4.squares.megadodo.umb/candidate-rows-and-columns/',
  write_compression = 'SNAPPY'
) AS
SELECT 
    row_number() OVER () AS id,
    t1.row_set AS candidate_rows,
    t1.repvalues AS repvalues_candidate_rows,
    t2.row_set AS candidate_columns,
    t2.repvalues AS repvalues_candidate_columns
FROM 
    magic_squares.candidate_rows t1
CROSS JOIN 
    magic_squares.candidate_rows t2
WHERE 
    NOT arrays_overlap(t1.repvalues, t2.repvalues)
    and cardinality(array_intersect(t1.row_set[1], t2.row_set[1])) = 1
    and cardinality(array_intersect(t1.row_set[1], t2.row_set[2])) = 1
    and cardinality(array_intersect(t1.row_set[1], t2.row_set[3])) = 1
    and cardinality(array_intersect(t1.row_set[1], t2.row_set[4])) = 1    
    and cardinality(array_intersect(t1.row_set[2], t2.row_set[1])) = 1
    and cardinality(array_intersect(t1.row_set[2], t2.row_set[2])) = 1
    and cardinality(array_intersect(t1.row_set[2], t2.row_set[3])) = 1
    and cardinality(array_intersect(t1.row_set[2], t2.row_set[4])) = 1        
    and cardinality(array_intersect(t1.row_set[3], t2.row_set[1])) = 1
    and cardinality(array_intersect(t1.row_set[3], t2.row_set[2])) = 1
    and cardinality(array_intersect(t1.row_set[3], t2.row_set[3])) = 1
    and cardinality(array_intersect(t1.row_set[3], t2.row_set[4])) = 1
    and cardinality(array_intersect(t1.row_set[4], t2.row_set[1])) = 1
    and cardinality(array_intersect(t1.row_set[4], t2.row_set[2])) = 1
    and cardinality(array_intersect(t1.row_set[4], t2.row_set[3])) = 1
    and cardinality(array_intersect(t1.row_set[4], t2.row_set[4])) = 1;
