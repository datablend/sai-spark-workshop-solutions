package be.datablend.spark.lab5;

import java.io.Serializable;

/**
 * Created by dsuvee
 * Class that encapsulates the review information
 */
class Review implements Serializable {

    private String review;
    private int stars;

    public Review(String review, int stars) {
        this.review = review;
        this.stars = stars;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public int getStars() {
        return stars;
    }

    public void setStars(int stars) {
        this.stars = stars;
    }

}
