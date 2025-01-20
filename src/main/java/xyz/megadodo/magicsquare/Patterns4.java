package xyz.megadodo.magicsquare;

public class Patterns4 {
    
    static int[][] pattern = {
        {2, 0, 1, 0},
        {1, 2, 0, 0},
        {0, 0, 2, 1},
        {0, 1, 0, 2}
    };

    static void transform(int [][] square)
    {
        //  For each row, get the "1" to the up diagonal
        int targetCol = 0;
        int startCol = 0;
        for(int row = 0; row < square.length; row++){
            for(int col = 0; col < square[row].length; col++){
                if(square[row][col] == 1){
                    targetCol = square.length - row - 1;
                    startCol = col;
                    break;
                }
            }
            //  swap columns
            for(int i = 0; i < square.length; i++){
                int tmp = square[i][startCol];
                square[i][startCol] = square[i][targetCol];
                square[i][targetCol] = tmp;
            }
            //  swap rows
            for(int j = 0; j < square.length; j++){
                int tmp = square[startCol][j];
                square[startCol][j] = square[targetCol][j];
                square[targetCol][j] = tmp;
            }
        }
    }

    static void checkPattern()
    {
        int [][] copy = new int[pattern.length][pattern.length];
        
        for(int i = 0; i < pattern.length; i++)
            for(int j = 0; j < pattern[i].length; j++)
                copy[i][j] = pattern[i][j];
        transform(copy);
                
        for(int i = 0; i < pattern.length; i++){
            System.out.print("[");
                for(int j = 0; j < pattern[i].length; j++)
                    System.out.print(pattern[i][j] + " ");
            System.out.print("]  ");
            System.out.print("[");
                for(int j = 0; j < copy[i].length; j++)
                    System.out.print(copy[i][j] + " ");
            System.out.println("]");
        }
    }

    static void init()
    {
        for(int i = 1; i < pattern.length; i++){
            for(int j = 0; j < i; j++)
                pattern[i][j] = 0;
        }
    }

    public static void main(String [] args)
    {
        for(int i = 1; i < pattern.length; i++){
            for(int j = 0; j < i; j++){

                int i2 = (j == (i - 1) ? i + 1: i);
                int j2 = 0;
                if(i2 < pattern.length){
                    j2 = (j == (i - 1) ? 0 : j + 1);
                }else
                    break;

                System.out.println("(i2, j2) = (" + i2 + ", " + j2 +")");

                for(int i3 = i2; i3 < pattern.length; i3++){
                    for(int j3 = j2; j3 < i2; j3++){
                        if(i3 == i && j3 == j)
                            continue;
                        System.out.println("(i3, j3) = (" + i3 + ", " + j3 +")");
                        init();
                        pattern[i][j] = 1;
                        pattern[i3][j3] = 1;
                        for(int i4 = 1; i4 < pattern.length; i4++)
                            for(int j4 = 0; j4 < i4; j4++)
                                pattern[j4][i4] = pattern[i4][j4];
                        checkPattern();
                        System.out.println("\n----------------\n");
                    }
                }

            }
        }
    }
}
