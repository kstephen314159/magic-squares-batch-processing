# Magic Squares of order 5

This repo contains the source code to an AWS-resource based approach to the computations needed to enumerate all the magic squares of order 5. While one could probably accomplish all of this on local, non-cloud resources, 
my experiences with computations needed for magic squares of order 4, and the known facts about the exploding numbers related to magic squares, make me believe that it would be very very difficult to accomplish on regular
personal computers. A distributed, and scalable approach is needed to address this problem.

##  An introduction to the problem: magic squares

Very simply, magic squares of order n are an n x n grid of (typically) consecutive numbers starting from 1 to n^2. All rows, columns, and diagonals will sum up to the same value - which is known as the magic sum. This is
n * (n^2 + 1) / 2 . One starts the problem by finding all the partitions of the magic sum using distinct terms, where the terms are drawn from the set 1, 2,....n^2. Then one groups these partitions into groups of n partitions,
where each group has the partitions such that no partition intersects with another partition in the same group. Each such group can be a candidate group of rows or a candidate group of columns. Each such group can then be 
used to derive a magic square by permuting the terms within the rows (or columns - depending on how you view the group) such that the columns now form partitions of the magic sum. The last step will be to validate that the
diagonals also sum upto the magic sum.

A different approach is to find a group - treat it as a row set, and to also find a corresponding second group and treat it as a column set. Now the problem will be to line up the diagonals. 

Before we go further, we need to define two terms: the up-diagonal and the down-diagonal. The down diagonal goes from the top left of the magic square to the bottom right. The up diagonal goes from the bottom left to the top right.

One can line up the down diagonal simply by finding a suitable permutation of the columns of the square. To line up the up diagonal, one can use permutations of rows and columns together. If you swap the position of columns x and y,
make the same swap for rows x and y. If you do this, there are known starting positions of the up diagonal which can be transformed to the right locations.
