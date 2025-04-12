CREATE TABLE "magic_squares"."candidate_magic_squares" WITH (
  format = 'parquet',
  external_location = 's3://stash.megadodo.umb/magic-squares/m4/candidate-rows/',
  write_compression = 'SNAPPY'
) as
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