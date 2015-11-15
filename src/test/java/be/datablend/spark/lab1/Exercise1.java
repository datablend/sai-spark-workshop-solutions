package be.datablend.spark.lab1;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Assert;
import org.junit.Test;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Create a distributed collection of integers and check that it effectively contains the required number of elements
    public void countNumberOfIntegers() {

        // Create a list of integers between 0 and 1.000.000
        List<Integer> integerList = new ArrayList<Integer>();
        for (int i = 0; i < 1000000; i++) {
            integerList.add(i);
        }

        // Make it a distributed collection
        JavaRDD<Integer> integerRDD = sc.parallelize(integerList);
        // Count the number of elements
        long count = integerRDD.count();

        // Check whether it actually contains a million elements
        Assert.assertEquals(1000000, count);
    }

    @Test
    // Create a distributed collection of integers and sum them all
    public void sumNumberOfIntegers() {

        // Create a list of integers between 0 and 1.000.000
        List<Integer> integerList = new ArrayList<Integer>();
        for (int i = 0; i < 1000000; i++) {
            integerList.add(i);
        }

        // Make it a distributed collection
        JavaRDD<Integer> integerRDD = sc.parallelize(integerList);
        // Sum the contained elements
        long sum = integerRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        // Check whether the sum equals 1783293664
        Assert.assertEquals(1783293664, sum);
    }

}
