package be.datablend.spark.lab4;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Keep the parsing time as a side metric while processing the coffee and tea words
    public void parsingTimeWhileProcessingCoffeeOrTeaWords() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Create an distributed counter (i.e. accumulator) that will keep track of the time
        final Accumulator<Integer> processingMillis = sc.accumulator(0);

        // Get all words associated with the review of a particular business
        JavaPairRDD<String, String> wordsOfBusinesses = reviewLines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                String[] elements = s.split("\t");

                // Setup a simple timer
                long start = System.currentTimeMillis();
                String[] words = elements[2].split("\\W");
                long stop = System.currentTimeMillis();
                // Add it to the accumulator
                processingMillis.add((int)(stop-start));

                List<Tuple2<String, String>> businessWords = new ArrayList<Tuple2<String, String>>();
                for (String word : words) {
                    businessWords.add(new Tuple2<String, String>(elements[1], word));
                }
                return businessWords;
            }
        });

        // Filter words on coffee or tea
        wordsOfBusinesses = wordsOfBusinesses.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return "coffee".equals(stringStringTuple2._2()) || "tea".equals(stringStringTuple2._2());
            }
        });

        // Group all words on business
        Map<String, Object> countsPerBusiness = wordsOfBusinesses.countByKey();

        // Check whether the number of coffee or tea words for business with id Oz1w_3Ck8lalmtxPcQMOIA is 55
        Assert.assertEquals(55L, countsPerBusiness.get("Oz1w_3Ck8lalmtxPcQMOIA"));

        // Get the value of the distributed counter
        System.out.println("Processing time: " + processingMillis.value());
    }

    @Test
    // Get the reviewed business with 5 stars but make use of broadcasting instead of joins to match up the business names
    public void getReviewedBusinessesWith5StarsWithNamesWithoutJoins() throws FileNotFoundException {
        // Parse the business names files separately
        Scanner scanner = new Scanner(new File(yelpBusinessesFileLocations));
        Map<String,String> mappings = new HashMap<String,String>();
        while (scanner.hasNextLine()) {
            String elements[] = scanner.nextLine().split("\t");
            mappings.put(elements[0],elements[1]);
        }

        // Broadcast this map of business id - business names mappings
        final Broadcast<Map<String,String>> broadcastedMappings = sc.broadcast(mappings);

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Filter them to 5-star businesses
        reviewLines = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });


        // Get the 5 stars for each business
        JavaPairRDD<String,Integer> businessStars = reviewLines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] elements = s.split("\t");
                // Use the broadcast variable to get the effective name of the business
                return new Tuple2<String, Integer>(broadcastedMappings.value().get(elements[1]),1);
            }
        });

        // Reduce by business
        businessStars = businessStars.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        // Find the value for the Postino Arcadia business
        List<Integer> businessCount = businessStars.lookup("Postino Arcadia");

        // Check whether it is effectively the Postino Acadia business
        Assert.assertEquals(473, (int)businessCount.get(0));
    }

    @Test
    // Get the mean number of stars for a particular business
    public void getMeanStarsForAParticularBusiness() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are going to try to find all reviews of business with id sgBl3UDEcNYKwuUb92CYdA
        JavaRDD<String> reviewsInterest = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "sgBl3UDEcNYKwuUb92CYdA".equals(elements[1]);
            }
        });

        // Get the star scores for this business
        JavaDoubleRDD starsOfInterest = reviewsInterest.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String s) throws Exception {
                String[] elements = s.split("\t");
                return Double.parseDouble(elements[3]);
            }
        });

        // Calculate the mean value
        double meanValue = starsOfInterest.mean();

        // Check whether it is 3.723404255319149
        Assert.assertEquals(3.723404255319149, meanValue, 0.0);
    }

}
