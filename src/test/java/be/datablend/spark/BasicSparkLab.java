package be.datablend.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

/**
 * Created by dsuvee
 */
public class BasicSparkLab {

    protected JavaSparkContext sc = null;
    // Locations to the data files
    protected String yelpReviewFileLocations = "/Users/dsuvee/sai/yelp_reviews.txt";
    protected String yelpBusinessesFileLocations = "/Users/dsuvee/sai/yelp_businesses.txt";

    @Before
    public void initializeSparkContext() {
        SparkConf sparkConf = new SparkConf().setAppName("SparkLabs");
        sparkConf.setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
    }

    @After
    public void closeSparkContext() {
        sc.close();
    }

}
