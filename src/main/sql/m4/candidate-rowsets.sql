CREATE TABLE "magic_squares"."candidate_rows" WITH (
  format = 'parquet',
  external_location = 's3://m4.squares.megadodo.umb/candidate-rows/',
  write_compression = 'SNAPPY'
) as
SELECT 
  arbitrary(row_set) AS row_set,
  array_sort(repValues) AS repValues
FROM (
  select distinct case
      when l2nm1.nomatch_row[1] > l2nm1.rows[1][1] and l2nm2.nomatch_row[1] > l2nm1.nomatch_row[1] then array [l2nm2.nomatch_row, l2nm1.nomatch_row, l2nm1.rows[1], l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] > l2nm1.rows[1][1] and l2nm2.nomatch_row[1] < l2nm1.nomatch_row[1] and l2nm2.nomatch_row[1] > l2nm1.rows[1][1] then array [l2nm1.nomatch_row, l2nm2.nomatch_row, l2nm1.rows[1], l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] > l2nm1.rows[1][1] and l2nm2.nomatch_row[1] < l2nm1.nomatch_row[1] and l2nm2.nomatch_row[1] > l2nm1.rows[2][1] then array [l2nm1.nomatch_row, l2nm1.rows[1], l2nm2.nomatch_row, l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] > l2nm1.rows[1][1] and l2nm2.nomatch_row[1] < l2nm1.rows[2][1] then array [l2nm1.nomatch_row, l2nm1.rows[1], l2nm1.rows[2], l2nm2.nomatch_row] 
      
      when l2nm1.nomatch_row[1] < l2nm1.rows[1][1] and l2nm1.nomatch_row[1] > l2nm1.rows[2][1] and l2nm2.nomatch_row[1] > l2nm1.rows[1][1] then array [l2nm2.nomatch_row, l2nm1.rows[1], l2nm1.nomatch_row, l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[1][1] and l2nm1.nomatch_row[1] > l2nm1.rows[2][1] and l2nm2.nomatch_row[1] < l2nm1.rows[1][1] and l2nm2.nomatch_row[1] > l2nm1.nomatch_row[1] then array [l2nm1.rows[1], l2nm2.nomatch_row, l2nm1.nomatch_row, l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[1][1] and l2nm1.nomatch_row[1] > l2nm1.rows[2][1] and l2nm2.nomatch_row[1] < l2nm1.nomatch_row[1] and l2nm2.nomatch_row[1] > l2nm1.rows[2][1] then array [l2nm1.rows[1], l2nm1.nomatch_row, l2nm2.nomatch_row, l2nm1.rows[2]] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[1][1] and l2nm1.nomatch_row[1] > l2nm1.rows[2][1] and l2nm2.nomatch_row[1] < l2nm1.rows[2][1] then array [l2nm1.rows[1], l2nm1.nomatch_row, l2nm1.rows[2], l2nm2.nomatch_row] 
      
      when l2nm1.nomatch_row[1] < l2nm1.rows[2][1] and l2nm2.nomatch_row[1] > l2nm1.rows[1][1] then array [l2nm2.nomatch_row, l2nm1.rows[1], l2nm1.rows[2], l2nm1.nomatch_row] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[2][1] and l2nm2.nomatch_row[1] < l2nm1.rows[1][1] and l2nm2.nomatch_row[1] > l2nm1.rows[2][1] then array [l2nm1.rows[1], l2nm2.nomatch_row, l2nm1.rows[2], l2nm1.nomatch_row] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[2][1] and l2nm2.nomatch_row[1] > l2nm1.nomatch_row[1] then array [l2nm1.rows[1], l2nm1.rows[2], l2nm2.nomatch_row, l2nm1.nomatch_row] 
      when l2nm1.nomatch_row[1] < l2nm1.rows[2][1] and l2nm2.nomatch_row[1] < l2nm1.nomatch_row[1] then array [l2nm1.rows[1], l2nm1.rows[2], l2nm1.nomatch_row, l2nm2.nomatch_row] 
      
    end row_set,
    l2nm1.repValues || l2nm1.noMatchRepValue || l2nm2.noMatchRepValue repValues
  from magic_squares.no_match_mapping_l2 l2nm1, magic_squares.no_match_mapping_l2 l2nm2
  where l2nm1.rows = l2nm2.rows
    and cardinality(array_intersect(l2nm1.nomatch_row, l2nm2.nomatch_row)) = 0
)
GROUP BY array_sort(repValues);