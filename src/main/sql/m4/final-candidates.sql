CREATE TABLE magic_squares.final_magic_square_candidates
WITH (
     format = 'PARQUET',
     external_location = 's3://m4.squares.megadodo.umb/final-magic-square-candidates/',
     write_compression = 'SNAPPY'
) AS
SELECT DISTINCT
    -- Logic 1: Swap Rows and Columns if max(col_rep) > max(row_rep)
    CASE 
        WHEN array_max(t1.repvalues_candidate_columns) > array_max(t1.repvalues_candidate_rows) 
        THEN t1.candidate_columns 
        ELSE t1.candidate_rows 
    END AS candidate_rows_final,
    
    CASE 
        WHEN array_max(t1.repvalues_candidate_columns) > array_max(t1.repvalues_candidate_rows) 
        THEN t1.candidate_rows 
        ELSE t1.candidate_columns 
    END AS candidate_columns_final,

    CASE 
        WHEN array_max(t1.repvalues_candidate_columns) > array_max(t1.repvalues_candidate_rows) 
        THEN t1.repvalues_candidate_columns 
        ELSE t1.repvalues_candidate_rows 
    END AS repvalues_candidate_rows_final,

    CASE 
        WHEN array_max(t1.repvalues_candidate_columns) > array_max(t1.repvalues_candidate_rows) 
        THEN t1.repvalues_candidate_rows 
        ELSE t1.repvalues_candidate_columns 
    END AS repvalues_candidate_columns_final,

    -- Logic 2: Swap Diagonals if diagonal_repvalue2 > diagonal_repvalue1
    CASE 
        WHEN t2.diagonal_repvalue > t1.diagonal_repvalue 
        THEN t2.diagonal_row 
        ELSE t1.diagonal_row 
    END AS up_diagonal,
    
    CASE 
        WHEN t2.diagonal_repvalue < t1.diagonal_repvalue
        THEN t2.diagonal_row 
        ELSE t1.diagonal_row 
    END AS down_diagonal

FROM 
    magic_squares.candidate_rows_columns_and_diagonal t1
INNER JOIN 
    magic_squares.candidate_rows_columns_and_diagonal t2
ON 
    t1.repvalues_candidate_rows = t2.repvalues_candidate_rows
    AND t1.repvalues_candidate_columns = t2.repvalues_candidate_columns
    AND t1.id != t2.id
WHERE 
    NOT arrays_overlap(t1.diagonal_row, t2.diagonal_row);