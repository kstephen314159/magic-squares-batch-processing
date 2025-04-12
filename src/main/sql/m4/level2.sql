CREATE TABLE "magic_squares"."no_match_mapping_l2" WITH (
  format = 'parquet',
  external_location = 's3://stash.megadodo.umb/magic-squares/m4/l2/',
  write_compression = 'SNAPPY'
) AS with nomatch_mapping(row1, repValues, row2, noMatchRepValue) as (
  SELECT distinct 
    p1.row,
    array [p1.repValue] repValues,
    p2.row, 
    p2.repValue
  FROM "magic_squares"."partitions" p1, "magic_squares"."partitions" p2
  where cardinality(array_intersect(p1.row, p2.row)) = 0
),
l2_nomatch_mapping(rows, repValues, nomatch_row, noMatchRepValue) as (
  select distinct case
      when nm1.row1[1] > nm1.row2[1] then array [ nm1.row1, nm1.row2 ]
      when nm1.row1[1] < nm1.row2[1] then array [ nm1.row2, nm1.row1 ]
    end,
    nm1.repValues || nm1.noMatchRepValue,
    nm2.row2, 
    nm2.noMatchRepValue
  from nomatch_mapping nm1, nomatch_mapping nm2
  where nm1.row1 = nm2.row1
    and cardinality(array_intersect(nm1.row2, nm2.row2)) = 0
)
select *
from l2_nomatch_mapping;