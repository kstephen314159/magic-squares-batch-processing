package xyz.megadodo.magicsquare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class PartitionTest {

    @Test
    void generatePartitions()
    {
        int [][] expectedPartitions = {
            {9, 5, 1},
            {9, 4, 2},
            {8, 6, 1},
            {8, 5, 2},
            {8, 4, 3},
            {7, 6, 2},
            {7, 5, 3},
            {6, 5, 4}
        };

        List<List<Integer>> expectedPartitionList = new ArrayList<List<Integer>>();
        for(int [] p: expectedPartitions)
            expectedPartitionList.add(Arrays.stream(p).boxed().toList());

        Partition pList = new Partition();
        List<List<Integer>> result = pList.getPartionsForSquare(15, 3);
        assertEquals(expectedPartitionList.size(), result.size());
        result.forEach(p -> {
            assertTrue(expectedPartitionList.contains(p));
            assertNotNull(pList.getRepresentativeValue(p));
        });
    }

    @Test
    void getPartitionsForSum()
    {
        try {
            String content = Files.readString(Path.of("src/test/data/partition-test-46-4-40.json"));
            Type listType = new TypeToken<ArrayList<List<Integer>>>(){}.getType();
            List<List<Integer>> expectedResult = (new Gson()).fromJson(content, listType);
            Partition pList = new Partition();
            try {
                List<List<Integer>> results = ((List<List<Integer>>)MethodUtils.invokeMethod(pList, true, "getPartitions", 46, 4, 40));
                assertEquals(expectedResult.size(), results.size());
                results.forEach(p -> {
                    assertTrue(expectedResult.contains(p));
                    /*
                     *  We cannot assert that the representative value is not null because we are invoking the private "getPartitions"
                     *  method, and the representative value is only set on the public method.
                     */
                });
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
