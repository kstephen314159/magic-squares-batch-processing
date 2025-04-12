package xyz.megadodo.magicsquare;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class Partition {

    private int squareOrder;
    private int magicSum;
    private long [] numberBitmaps;
    Hashtable<List<Integer>, Long> partitionRepresentativeValues = new Hashtable<List<Integer>, Long>();

    public Partition(int magicSQuareOrder)
    {
        this.squareOrder = magicSQuareOrder;
        numberBitmaps = new long[magicSQuareOrder * magicSQuareOrder + 1];
        magicSum = magicSQuareOrder * (magicSQuareOrder * magicSQuareOrder + 1) / 2;
        numberBitmaps[0] = 0;
        numberBitmaps[1] = 1;
        for(int i = 2; i < numberBitmaps.length; i++)
            numberBitmaps[i] = numberBitmaps[i - 1] << 1;
    }

    public List<List<Integer>> getPartionsForSquare(){

        /*
         *  The number of distinct partitions of "sum" with exactly "numTerms" terms where the
         *  terms are drawn from 1 to (including) numTerms^2 is what we are seeking. We start
         *  by finding the leftmost term by iteratively trying values by incrementing downwards
         *  from numTerms^2. We don't need to iterate down all the way to 1. This is because
         *  we will never find a partition with the right sum once we hit the value (1 + 2 +
         *  ... + (numTerms - 1). This is equal to numTerms * (numTerms - 1) / 2
         */
        int minBound = squareOrder * (squareOrder - 1) / 2;
        int largestTerm = squareOrder * squareOrder;

        List<List<Integer>> result = new ArrayList<List<Integer>>();
        for(int outerMost = largestTerm; outerMost > minBound; outerMost--){
            final int firstTerm = outerMost;
            getPartitions(magicSum - outerMost, squareOrder - 1, outerMost - 1).forEach(partition -> {
                partition.add(0, firstTerm);
                long representativeValue = 0;
                for(int i = 0; i < partition.size(); i++)
                    representativeValue |= numberBitmaps[partition.get(i)];
                partitionRepresentativeValues.put(partition, representativeValue);
                result.add(partition);
            });
        }
        return result;
    }

    private List<List<Integer>> getPartitions(int sum, int numTerms, int largestTerm){

        List<List<Integer>> result = new ArrayList<List<Integer>>();
        if(numTerms == 2){
            /*
             *  When looking at the sum of two numbers, where the max number is x, the largest
             *  sum we can get is (x - 1) + x = 2*x -1 . Since the largest value of x is 
             *  "largestTerm", if 2*largestTerm - 1 < sum, there is no partition that we can 
             *  form.
             * 
             *  We can bound this largestTerm even more. See explanation in getPartitionsForSquare
             *  for what "minBound" is. Sometimes, (sum - minBound) is smaller than the largest
             *  term. In that case, we don't need to try to find partitions starting with
             *  largestTerm. Instead, we can start with (sum - minBound).
             * 
             *  Example: suppose we are finding partitions of 15 where the candidate terms range
             *  from 1 to 9. If this method were called when we were finding the partitions where
             *  the largest term were 9, then the arguments would be sum = 6, numTerms = 2, and 
             *  largestTerm = 8. Since minBound would be 1, we don't need to try the 8 + 1 or
             *  7 + 1 or 6 + 1 additions to see if they are valid sums. Instead we can start with 
             *  6 - 1 = 5 as the first possible largest term.
             * 
             *  When numTerms = 2, then minBound = 1.
             */
            int maxBound = ((sum - 1) < largestTerm)? (sum - 1) : largestTerm;
            if(2 * maxBound - 1 < sum)
                return result;

            for(int i = maxBound; i > 1; i--)
                for(int j = i - 1; j > 0; j--){
                    if(i + j == sum){
                        List<Integer> partition = new ArrayList<Integer>();
                        partition.add(i);
                        partition.add(j);
                        result.add(partition);
                        break;
                    }
                }
        }else{
            int minBound = numTerms * (numTerms - 1) / 2;

            for(int outerMost = largestTerm; outerMost >= minBound; outerMost--){
                final int firstTerm = outerMost;
                if(sum < outerMost)
                    continue;
                getPartitions(sum - outerMost, numTerms - 1, outerMost - 1).forEach(partition -> {
                    partition.add(0, firstTerm);
                    result.add(partition);
                });
            }
        }
        return result;
    }

    public long getRepresentativeValue(List<Integer> partition)
    {
        return partitionRepresentativeValues.get(partition);
    }
}
