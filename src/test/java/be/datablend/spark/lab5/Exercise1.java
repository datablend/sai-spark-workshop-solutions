package be.datablend.spark.lab5;

import be.datablend.spark.BasicSparkLab;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by dsuvee
 */
public class Exercise1 extends BasicSparkLab implements Serializable {

    @Test
    // Compute the tf-idf values for all reviews
    public void computeTFIDF() {
        // Import data from the review file (let's samples 10% of it)
        JavaRDD<String> reviewLines = sc.textFile(yelpReviewFileLocations).sample(true,0.1);

        // Create a new data structure where every review has its own id, review and score
        JavaPairRDD<String,Review> reviews = reviewLines.mapToPair(new PairFunction<String, String, Review>() {
            @Override
            public Tuple2<String, Review> call(String s) throws Exception {
                String[] elements = s.split("\t");
                String reviewText = elements[2];
                int stars = Integer.parseInt(elements[3]);
                Review review = new Review(reviewText,stars);
                // We don't really have a UUID for a review specifically. So let's just create one on the fly
                String id = UUID.randomUUID().toString();
                return new Tuple2<String, Review>(id, review);
            }
        });

        // Count the number of reviews
        final long reviewCount = reviews.count();

        // Calculate the tf values for each word within a particular review. At the end, this will contains <word#review_id,tf> values
        JavaPairRDD<String,Integer> tfValues = reviews.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Review>, String, Integer>() {
            @Override
            public List<Tuple2<String, Integer>> call(Tuple2<String, Review> reviewTuple) throws Exception {
                String[] words = reviewTuple._2().getReview().split("\\W");
                List<Tuple2<String, Integer>> theReviewWords = new ArrayList<Tuple2<String, Integer>>();
                for (String word : words) {
                    theReviewWords.add(new Tuple2<String, Integer>(word.toLowerCase() + "#" + reviewTuple._1(), 1));
                }
                return theReviewWords;
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        // Let's map to word - <docid,tf> tuples, which allows us to count the number of document having a particular word
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> mappedTfValues = tfValues.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String[] elements = stringIntegerTuple2._1().split("#");
                String word = elements[0];
                String docId = elements[1];
                int tf = stringIntegerTuple2._2();
                return new Tuple2<String, Tuple2<String, Integer>>(word, new Tuple2<String, Integer>(docId, tf));
            }
        }).groupByKey();

        // Lets flatmap to <word#document, tfidf values)
        JavaPairRDD<String, Double> tfidfs = mappedTfValues.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Double>() {
            @Override
            public Iterable<Tuple2<String, Double>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                // Get the words
                String word = stringIterableTuple2._1();

                // Get the tfs
                List<Tuple2<String, Integer>> tfs = new ArrayList<Tuple2<String, Integer>>();
                CollectionUtils.addAll(tfs, stringIterableTuple2._2().iterator());

                // Number of documents having the word and calculate the idf for that word
                int n = tfs.size();
                double idf = Math.log10((double)reviewCount/n);

                // Calculate the tfidfs
                List<Tuple2<String, Double>> tfidfs = new ArrayList<Tuple2<String, Double>>();
                for (Tuple2<String, Integer> tf : tfs) {
                    String document = tf._1();
                    Double tfidf = tf._2() * idf;
                    tfidfs.add(new Tuple2<String, Double>(word + "#" + document, tfidf));
                }
                return tfidfs;
            }
        });

        // Inspect the values
        List<Tuple2<String, Double>> tfidfvalues = tfidfs.take(10);

    }

}
