CREATE TABLE "magic_squares"."no_match_mapping_l3" WITH (
  format = 'parquet',
  external_location = 's3://stash.megadodo.umb/magic-squares/l3/',
  write_compression = 'SNAPPY'
) AS with l3_nomatch_mapping(rows, repValues, nomatch_row, noMatchRepValue) as (
  select distinct case
      when l2nm1.rows[1][1] > l2nm1.nomatch_row[1] and l2nm2.nomatch_row [1] > l2nm1.rows [2][1] then array [ l2nm1.rows[1], l2nm1.nomatch_row, l2nm1.rows[2] ]
      when l2nm1.nomatch_row[1] < l2nm1.rows[2][1] then array [ l2nm1.rows[1], l2nm1.rows[2], l2nm1.nomatch_row ]
      when l2nm1.rows[1][1] < l2nm1.nomatch_row[1] then array [ l2nm1.nomatch_row, l2nm1.rows[1], l2nm1.rows[2] ]
    end,
    l2nm1.repValues || l2nm1.noMatchRepValue,
    l2nm2.nomatch_row, 
    l2nm2.noMatchRepValue
  from magic_squares.no_match_mapping_l2 l2nm1, magic_squares.no_match_mapping_l2 l2nm2
  where l2nm1.rows = l2nm2.rows
    and cardinality(array_intersect(l2nm1.nomatch_row, l2nm2.nomatch_row)) = 0
)
select *
from l3_nomatch_mapping;