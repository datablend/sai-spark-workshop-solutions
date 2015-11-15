package be.datablend.spark.lab2;

import be.datablend.spark.BasicSparkLab;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.Assert;
import org.junit.Test;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Count the number of reviews in the file
    public void getReviewedBusinessesForEachReviewer() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Count the number of reviews
        long count = reviewLines.count();

        // Check whether it actually contains 335022 reviews
        Assert.assertEquals(335022, count);
    }

    @Test
    // Count the number of stars
    public void countNumberOfStars() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are not interested in all data, just the star data (so the fourth column in our dataset)
        JavaRDD<String> stars = reviewLines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[3];
            }
        });

        // Count the stars
        long count = stars.count();

        // Check whether it actually contains 335022 stars
        Assert.assertEquals(335022, count);
    }

    @Test
    // Count the number of reviews that gave 5 stars
    public void countNumberOfFiveStarReviews() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are not interested in all data, just the stars themselves
        JavaRDD<String> stars = reviewLines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[3];
            }
        });

        // Filter the star data on the ones that equal "5"
        JavaRDD<String> fiveStars = stars.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
               return "5".equals(s);
            }
        });

        // Count the stars
        long count = fiveStars.count();

        // Check whether it actually contains 119389 five star reviews
        Assert.assertEquals(119389, count);
    }

    @Test
    // Calculate the percentage of 3-star reviews on the full review data set
    public void calculatePercentualNumberOfThreeStarReviews() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Count the total number of reviews
        long totalCount = reviewLines.count();

        // We are not interested in all data, just the stars themselves
        JavaRDD<String> stars = reviewLines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[3];
            }
        });

        // Filter the star data on the ones that equal "3"
        JavaRDD<String> threeStars = stars.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return "3".equals(s);
            }
        });

        // Count the stars
        long count = threeStars.count();

        // Calculate the percentage
        double percentage = (double)count / totalCount;

        // Check whether it's the actual percentage
        Assert.assertEquals(0.14208022159738765, percentage, 0.0);
    }

    @Test
    // Count the number of business that have been granted 5 star reviews
    public void countNumberOfFiveStarBusinesses() {
        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are not interested in businesses, just the businesses that have 5 stars
        JavaRDD<String> fiveStarReviews = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });

        // Let's extract the ids of those 5-star businesses
        JavaRDD<String> fiveStarBusinesses = fiveStarReviews.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[1];
            }
        });

        // Som businesses could have receive multiple 5-star reviews. We need to keep the distinct ones
        JavaRDD<String> uniqueFiveStarBusinesses = fiveStarBusinesses.distinct();

        // Count the number of unique 5-star businesses
        long count = uniqueFiveStarBusinesses.count();

        // Check whether it actually contains 12698 businesses
        Assert.assertEquals(12698, count);
    }

    @Test
    // Cound the number of business that have received both 1-star and 5-star reviews (use the intersection transformation operator)
    public void countNumberOfCombinedOneStarAndFiveStarBusinesses() {

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are not interested in all businesses, just the businesses that have 5 stars
        JavaRDD<String> fiveStarReviews = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });

        // Let's now extract the business ids
        JavaRDD<String> fiveStarBusinesses = fiveStarReviews.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[1];
            }
        }).distinct();

        // We are not interested in all businesses, just the businesses that have 1 stars
        JavaRDD<String> oneStarReviews = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "1".equals(elements[3]);
            }
        });

        // Let's now extract the business ids
        JavaRDD<String> oneStarBusinesses = oneStarReviews.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[1];
            }
        }).distinct();

        // We want to have the business that have both 1 star and 5 star reviews
        JavaRDD<String> bothOneAndFiveStarBusinesses = oneStarBusinesses.intersection(fiveStarBusinesses);

        // Count the number of businesses still included
        long count = bothOneAndFiveStarBusinesses.count();

        // Check whether it actually contains 7002 businesses
        Assert.assertEquals(7002, count);
    }

    @Test
    // Count the number of times the word "chinese" is mentioned in a review
    public void countNumberOfChineseWordMentions() {

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // Get all words from the review
        JavaRDD<String> reviewWords = reviewLines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] elements = s.split("\t");
                String[] words = elements[2].split("\\W");
                return Arrays.asList(words);
            }
        });

        // Filter those words on chines
        JavaRDD<String> chineseWords = reviewWords.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return "chinese".equals(s);
            }
        });

        // Count the chinese word mentions
        long count = chineseWords.count();

        // Check whether it actually contains 1469 chinese word mentions
        Assert.assertEquals(1469, count);

        // What if you would like to quickly execute this on just sample of the data?
    }

    @Test
    // Get a local list of all reviewers of a particular business with id sgBl3UDEcNYKwuUb92CYdA
    public void retrieveAllReviewersOfAParticularBusiness() {

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are going to try to find all reviewers of business with id sgBl3UDEcNYKwuUb92CYdA
        JavaRDD<String> reviewsInterest = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "sgBl3UDEcNYKwuUb92CYdA".equals(elements[1]);
            }
        });

        // Get the review ids
        JavaRDD<String> reviewersOfInterest = reviewsInterest.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] elements = s.split("\t");
                return elements[0];
            }
        });

        // Get them to the driver
        List<String> nonDistributedReviewersOfInterest = reviewersOfInterest.distinct().collect();

        // Count our reviewers of interest
        long count = nonDistributedReviewersOfInterest.size();

        // Check whether it actually contains 136 reviewers
        Assert.assertEquals(136, count);

        // What if we are only interested in the first 10?
    }

    @Test
    // Count the number of mentionings of the words coffee or tea in 5-star reviews (use the union transformation operator)
    public void countCoffeeOrTeaMentionsIn5StarReviews() {

        // Import data from the review file
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations);

        // We are not interested in all business reviews, just the reviews of businesses that have 5 stars
        JavaRDD<String> fiveStarReviews = reviewLines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] elements = s.split("\t");
                return "5".equals(elements[3]);
            }
        });

        // Get all words from these review
        JavaRDD<String> reviewWords = fiveStarReviews.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] elements = s.split("\t");
                String[] words = elements[2].split("\\W");
                return Arrays.asList(words);
            }
        });

        // Find the Coffee mentions
        JavaRDD<String> coffeeWords = reviewWords.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return "coffee".equals(s);
            }
        });

        // Find the Tea mentions
        JavaRDD<String> teaWords = reviewWords.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return "tea".equals(s);
            }
        });

        JavaRDD<String> coffeeOrTeaWords = coffeeWords.union(teaWords);

        // Count the number of occurrences
        long count = coffeeOrTeaWords.count();

        // Check whether it actually contains 11596 occurrences
        Assert.assertEquals(11596, count);
    }

}
