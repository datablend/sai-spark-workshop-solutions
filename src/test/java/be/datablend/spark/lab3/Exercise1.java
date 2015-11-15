package be.datablend.spark.lab3;

import be.datablend.spark.BasicSparkLab;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Get the reviewed businesses for each reviewer
    public void getReviewedBusinessesForEachReviewer() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Generate the reviewer-business pairs
        JavaPairRDD<String,String> reviewerBusinessPairs = reviewLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, String>(elements[0],elements[1]);
            }
        }).distinct();

        // Group the key-value paiers on key (so the reviewer itself)
        JavaPairRDD<String, Iterable<String>> groupedOnReviewer = reviewerBusinessPairs.groupByKey();

        // Get the ones of reviewer JhnjcLJM5hiXK6R4minDsA
        Iterable<String> businesses = groupedOnReviewer.lookup("JhnjcLJM5hiXK6R4minDsA").get(0);
        List<String> businessIds = new ArrayList<String>();
        CollectionUtils.addAll(businessIds,businesses.iterator());

        // Check whether user JhnjcLJM5hiXK6R4minDsA contains a review for business B8ujMtvvpHyEQ2r_QlAT2w
        Assert.assertTrue(businessIds.contains("B8ujMtvvpHyEQ2r_QlAT2w"));
    }

    @Test
    // Count the reviewed businesses for each reviewer
    public void countReviewedBusinessesForEachReviewer() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Get the review pairs
        JavaPairRDD<String,String> reviewerBusinessPairs = reviewLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, String>(elements[0],elements[1]);
            }
        });

        // Count the reviews based upon the reviewer key
        Map<String,Object> reviewerData = reviewerBusinessPairs.countByKey();

        // Test it for JhnjcLJM5hiXK6R4minDsA
        long count = (Long)reviewerData.get("JhnjcLJM5hiXK6R4minDsA");

        // Check whether user JhnjcLJM5hiXK6R4minDsA actually has written 5 reviews
        Assert.assertEquals(5, count);
    }

    @Test
    // Count the number of coffee or tea mentions per business
    public void countNumberOfCoffeeAndTeaMentionsPerBusinesses() {

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Get all words associated with the review of a particular business
        JavaPairRDD<String,String> wordsOfBusinesses = reviewLines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                String[] elements = s.split("\t");
                String[] words = elements[2].split("\\W");
                List<Tuple2<String,String>> businessWords = new ArrayList<Tuple2<String,String>>();
                for (String word : words) {
                    businessWords.add(new Tuple2<String, String>(elements[1],word));
                }
                return businessWords;
            }
        });

        // Filter business-words tuples on the ones that contains the word coffee or tea
        wordsOfBusinesses = wordsOfBusinesses.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return "coffee".equals(stringStringTuple2._2()) || "tea".equals(stringStringTuple2._2());
            }
        });

        // Group all words on the business id key
        Map<String, Object> countsPerBusiness = wordsOfBusinesses.countByKey();

        // Check whether the number of coffee or tea words mentions for business with id Oz1w_3Ck8lalmtxPcQMOIA is 55
        Assert.assertEquals(55L, countsPerBusiness.get("Oz1w_3Ck8lalmtxPcQMOIA"));
    }

    @Test
    // Get the businesses with the most 5-star reviews and find the number 1
    public void getTopReviewedBusinessesWith5Stars() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Filter the reviews on only 5-star reviews
        reviewLines = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });

        // Make key-value pairs that allows us to afterwards count the number of 5-star reviews for each business
        JavaPairRDD<String,Integer> businessStars = reviewLines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, Integer>(elements[1],1);
            }
        });

        // Reduce by key (in this case on business)
        businessStars = businessStars.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        // We want to sort on the value. Let's swap key and value. This allows us to sort on the count per business
        JavaPairRDD<Integer,String> swappedBusinessStars = businessStars.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2(stringIntegerTuple2._2,stringIntegerTuple2._1);
            }
        });

        // Sort on key (descending). The top business will be the first one
        swappedBusinessStars = swappedBusinessStars.sortByKey(false);

        // Get the best one
        Tuple2<Integer, String> top = swappedBusinessStars.first();

        // Check whether it is effectively the business with id SDwYQ6eSu1htn8vHWv128g
        Assert.assertEquals("SDwYQ6eSu1htn8vHWv128g", top._2());
    }

    @Test
    // Calculate the average stars per business
    public void getAverageStarsPerBusiness() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Get the stars per business (business-star pairs)
        JavaPairRDD<String,Integer> businessStars = reviewLines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, Integer>(elements[1],Integer.parseInt(elements[3]));
            }
        });

        // For each business, map the star to a pair of value and 1. This allows us to simultanously add the values and occurrencess
        JavaPairRDD<String,Tuple2<Integer,Integer>> countedBusinessStars = businessStars.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, 1);
            }
        });

        // Reduce on key. This will sum the stars but at the same time count the occurrences
        countedBusinessStars = countedBusinessStars.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple1, Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return new Tuple2<Integer,Integer>(integerIntegerTuple1._1 + integerIntegerTuple2._1, integerIntegerTuple1._2 + integerIntegerTuple2._2);
            }
        });

        // Average the sum for each business
        JavaPairRDD<String,Double> businessStarsAverages = countedBusinessStars.mapValues(new Function<Tuple2<Integer,Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return (double)integerIntegerTuple2._1() / integerIntegerTuple2._2();
            }
        });

        // Get the average for business with id sgBl3UDEcNYKwuUb92CYdA
        double average = businessStarsAverages.lookup("sgBl3UDEcNYKwuUb92CYdA").get(0);

        // Check whether it is 3.723404255319149 for business sgBl3UDEcNYKwuUb92CYdA
        Assert.assertEquals(3.723404255319149, average, 0.0);
    }

    @Test
    // Get the real names of the business (instead of just the ids) for each 5-star reviewed business (use the join transformation)
    public void getReviewedBusinessesWith5StarsWithNames() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Filter on the 5-star reviews
        reviewLines = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });

        // Get the 5 stars for each business (business-1 pairs)
        JavaPairRDD<String,Integer> businessStars = reviewLines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, Integer>(elements[1],1);
            }
        });

        // Reduce by business key
        businessStars = businessStars.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        // Read the business file names
        JavaRDD<String> businessLines = sc.textFile(yelpBusinessesFileLocations);

        // For each business, map the business id to its actual name
        JavaPairRDD<String,String> businessNames = businessLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] elements = s.split("\t");
                return new Tuple2<String, String>(elements[0],elements[1]);
            }
        });

        // Join both datasets on business id
        JavaPairRDD<String, Tuple2<Integer, String>> combined = businessStars.join(businessNames);

        // Find the tuple for the SDwYQ6eSu1htn8vHWv128g business
        List<Tuple2<Integer, String>> businessTuple = combined.lookup("SDwYQ6eSu1htn8vHWv128g");

        // Check whether it is effectively the Postino Acadia business
        Assert.assertEquals("Postino Arcadia", businessTuple.get(0)._2());

        // What if we want to store the results?
        //combined.saveAsTextFile("/Users/dsuvee/sai/output.txt");

    }

}
