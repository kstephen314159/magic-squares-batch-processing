package xyz.megadodo.magicsquare;

import java.math.BigInteger;

public class Oddity {
    
    public static void main(String [] args)
    {
        int maxx = 3;
        int maxz=maxx*(maxx-1);
        int maxy=maxx*maxx*(maxx-1)/2;
        int numx=1;
        BigInteger rArray[][] = new BigInteger[maxx+1][maxy+1];  // this is large and growing and a potential memory problem
        for (int x=0; x<=maxx; x++){
            for (int y=maxy; y>=0; y--){
                rArray[x][y]=BigInteger.valueOf(0l);   //start with all zero, except ... 
            }
        }
        rArray[0][0]=BigInteger.valueOf(1l);             //... except for a single one. 

        for (int z=0; z<=maxz; z++){
            for (int x=1; x<=maxx; x++){        //Need to increase to use previous values
                for (int y=maxy; y>=0; y--){    //Need to decrease to use previous values
                    if (y>=x){
                        rArray[x][y]=rArray[x-1][y].add(rArray[x][y-x]);  //the recurrence: rArray[x-1][y] is new while rArray[x][y-x] is old 
                    }else{
                        rArray[x][y]=rArray[x-1][y];  // rArray[x][y-x] does not exist if x>y
                    }
                }
                System.out.println("---------------");
                for(BigInteger[] row: rArray){
                    for(BigInteger cell: row)
                        System.out.print(cell + ",");
                    System.out.println("");
                }
            }
            if (numx*(numx-1)<=z){
                System.out.println(Integer.toString(numx) + "    " + rArray[numx][numx*numx*(numx-1)/2].toString() + " -- " + z);
                System.out.println("------  results  ---------");
                for(BigInteger[] row: rArray){
                    for(BigInteger cell: row)
                        System.out.print(cell + ",");
                    System.out.println("");
                }
                numx++;
            }    // end of if/messagebox 
        }        // end of z loop 
    }
}
