CREATE TABLE "magic_squares"."candidate_magic_squares" WITH (
  format = 'parquet',
  external_location = 's3://stash.megadodo.umb/magic-squares/candidate-rows/',
  write_compression = 'SNAPPY'
) as
select distinct case
    when l3nm1.nomatch_row[1] > l3nm1.rows[1][1] and l3nm2.nomatch_row[1] > l3nm1.nomatch_row[1] then array [l3nm2.nomatch_row, l3nm1.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] > l3nm1.rows[1][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] and l3nm2.nomatch_row[1] > l3nm1.rows[1][1] then array [l3nm1.nomatch_row, l3nm2.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] > l3nm1.rows[1][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] and l3nm2.nomatch_row[1] > l3nm1.rows[2][1] then array [l3nm1.nomatch_row, l3nm1.rows[1], l3nm2.nomatch_row, l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] > l3nm1.rows[1][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] and l3nm2.nomatch_row[1] > l3nm1.rows[3][1] then array [l3nm1.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm2.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] > l3nm1.rows[1][1] and l3nm2.nomatch_row[1] < l3nm1.rows[3][1] then array [l3nm1.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3], l3nm2.nomatch_row] 
    
    when l3nm1.nomatch_row[1] < l3nm1.rows[1][1] and l3nm1.nomatch_row[1] > l3nm1.rows[2][1] and l3nm2.nomatch_row[1] > l3nm1.rows[1][1] then array [l3nm2.nomatch_row, l3nm1.rows[1], l3nm1.nomatch_row, l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[1][1] and l3nm1.nomatch_row[1] > l3nm1.rows[2][1] and l3nm2.nomatch_row[1] < l3nm1.rows[1][1] and l3nm2.nomatch_row[1] > l3nm1.nomatch_row[1] then array [l3nm1.rows[1], l3nm2.nomatch_row, l3nm1.nomatch_row, l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[1][1] and l3nm1.nomatch_row[1] > l3nm1.rows[2][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] and l3nm2.nomatch_row[1] > l3nm1.rows[2][1] then array [l3nm1.rows[1], l3nm1.nomatch_row, l3nm2.nomatch_row, l3nm1.rows[2], l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[1][1] and l3nm1.nomatch_row[1] > l3nm1.rows[2][1] and l3nm2.nomatch_row[1] < l3nm1.rows[2][1] and l3nm2.nomatch_row[1] > l3nm1.rows[3][1] then array [l3nm1.rows[1], l3nm1.nomatch_row, l3nm1.rows[2], l3nm2.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[1][1] and l3nm1.nomatch_row[1] > l3nm1.rows[2][1] and l3nm2.nomatch_row[1] < l3nm1.rows[3][1] then array [l3nm1.rows[1], l3nm1.nomatch_row, l3nm1.rows[2], l3nm1.rows[3], l3nm2.nomatch_row] 
    
    when l3nm1.nomatch_row[1] < l3nm1.rows[2][1] and l3nm1.nomatch_row[1] > l3nm1.rows[3][1] and l3nm2.nomatch_row[1] > l3nm1.rows[1][1] then array [l3nm2.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm1.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[2][1] and l3nm1.nomatch_row[1] > l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[1][1] and l3nm2.nomatch_row[1] > l3nm1.rows[2][1] then array [l3nm1.rows[1], l3nm2.nomatch_row, l3nm1.rows[2], l3nm1.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[2][1] and l3nm1.nomatch_row[1] > l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[2][1] and l3nm2.nomatch_row[1] > l3nm1.nomatch_row[1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm2.nomatch_row, l3nm1.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[2][1] and l3nm1.nomatch_row[1] > l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] and l3nm2.nomatch_row[1] > l3nm1.rows[3][1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm1.nomatch_row, l3nm2.nomatch_row, l3nm1.rows[3]] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[2][1] and l3nm1.nomatch_row[1] > l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[3][1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm1.nomatch_row, l3nm1.rows[3], l3nm2.nomatch_row] 

    when l3nm1.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] > l3nm1.rows[1][1] then array [l3nm2.nomatch_row, l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3], l3nm1.nomatch_row] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[1][1] and l3nm2.nomatch_row[1] > l3nm1.rows[2][1] then array [l3nm1.rows[1], l3nm2.nomatch_row, l3nm1.rows[2], l3nm1.rows[3], l3nm1.nomatch_row] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[2][1] and l3nm2.nomatch_row[1] > l3nm1.rows[3][1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm2.nomatch_row, l3nm1.rows[3], l3nm1.nomatch_row] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] > l3nm1.nomatch_row[1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3], l3nm2.nomatch_row, l3nm1.nomatch_row] 
    when l3nm1.nomatch_row[1] < l3nm1.rows[3][1] and l3nm2.nomatch_row[1] < l3nm1.nomatch_row[1] then array [l3nm1.rows[1], l3nm1.rows[2], l3nm1.rows[3], l3nm1.nomatch_row, l3nm2.nomatch_row] 
    
  end row_set,
  l3nm1.repValues || l3nm1.noMatchRepValue || l3nm2.noMatchRepValue repValues
from magic_squares.no_match_mapping_l3 l3nm1, magic_squares.no_match_mapping_l3 l3nm2
where l3nm1.rows = l3nm2.rows
  and cardinality(array_intersect(l3nm1.nomatch_row, l3nm2.nomatch_row)) = 0