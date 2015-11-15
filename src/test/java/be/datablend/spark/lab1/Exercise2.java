package be.datablend.spark.lab1;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Assert;
import org.junit.Test;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dsuvee
 */
public class Exercise2 extends BasicSparkLab implements Serializable {

    @Test
    // Create a distributed collection of sentences and filter the ones that do not contain spark
    public void filterSentencesAndCount() {
        // We create a list of strings (basically our sentences)
        List<String> input = new ArrayList<String>();
        input.add("spark is great");
        input.add("hadoop is great");
        input.add("spark beats hadoop");

        // Make it a distributed collection of strings
        JavaRDD<String> rdd = sc.parallelize(input);

        // Filter the sentences. Only keep the ones that contain "spark"
        JavaRDD<String> filteredRDD = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("spark");
            }
        });

        // Count the results
        long count = filteredRDD.count();

        // Check whether it actually contains 2 sentences
        Assert.assertEquals(2, count);
    }

}
