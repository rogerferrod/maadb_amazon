import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SparkSQL {

    public static void main(String[] args) throws IOException {
        String url = args[5];
        String user = args[6];
        String psw = args[7];

        Encoder<Review> reviewEncoder = Encoders.bean(Review.class);

        // Create the SparkSession.
        System.out.println("Initialize project");
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQL")
                //.config("spark.master", "local")
                .getOrCreate();

        // Create JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Initialize Broadcast variables
        final Broadcast<Map<String, Float>> lexicon = jsc.broadcast(Vader.makeLexiconDictionary(args[0]));
        final Broadcast<Map<String, Float>> emoticon = jsc.broadcast(Vader.makeEmoticonDictionary(args[1]));
        final Broadcast<Map<String, String>> slang = jsc.broadcast(Vader.makeSlangDictionary(args[2]));
        final Broadcast<Map<String, Float>> modifier = jsc.broadcast(Vader.makeModifierDictionary(args[3]));
        final Broadcast<ArrayList<String>> negate = jsc.broadcast(Vader.makeNegateList(args[4]));

        // Application logic
        Dataset<Row> reviewsRaw = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", psw)
                .option("dbtable", "reviews")
                .load();

        //reviewsRaw = reviewsRaw.sample(0.01);
        Dataset<Review> reviews = reviewsRaw.as(reviewEncoder);

        Dataset<Review> reviewsScore = reviews.map((MapFunction<Review, Review>) review -> {
                    try {
                        String reviewText = review.getReviewText();
                        String reviewTitle = review.getTitle();

                        if (reviewText.length() > 0 && reviewTitle.length() > 0) {
                            List<String> sentences = Vader.tokenizeSentence(reviewText);
                            Vader vaderText = new Vader(lexicon.value(), emoticon.value(), slang.value(), modifier.value(), negate.value(), sentences);
                            Vader vaderTitle = new Vader(lexicon.value(), emoticon.value(), slang.value(), modifier.value(), negate.value(), reviewTitle);
                            float textScore = vaderText.evaluate();
                            float titleScore = vaderTitle.evaluate();
                            double tot = (textScore + titleScore) / 2;

                            review.setPolarity(tot);
                        } else {
                            review.setPolarity(null);
                        }
                    } catch (Exception e) {
                        System.out.println("error " + e);
                    }
                    return review;
                },
                Encoders.bean(Review.class));

        reviewsScore = reviewsScore.filter((FilterFunction<Review>) review -> review.getPolarity() != null);
        reviewsScore.cache(); //caching
        reviewsScore.createOrReplaceTempView("score_reviews");

        // Write results
        System.out.println("Writing scores...");
        reviewsScore.write().format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", psw)
                .option("dbtable", "score_reviews")
                .save();

        // Join
        System.out.println("Computing Join...");
        Dataset<Row> productsRaw = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", psw)
                .option("dbtable", "products")
                .load();

        productsRaw.createOrReplaceTempView("products");

        Dataset<Row> result = spark.sql("select s.asin as id, p.description, p.title, avg(s.polarity) as polarityAVG\n" +
                "from score_reviews as s join products as p on p.id=s.asin\n" +
                "where polarity is not null\n" +
                "group by asin,description,p.title\n" +
                "order by s.asin");

        // Write results
        System.out.println("Writing products...");
        result.write().format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", psw)
                .option("dbtable", "products_score")
                .save();

        // Pearson index
        System.out.println("Computing Pearson index...");
        JavaRDD<Row> selectedRows = reviewsScore.select("polarity", "overall").javaRDD();
        JavaPairRDD<Double, Double> vectors = selectedRows.mapToPair(row -> new Tuple2<>((Double) row.get(0), (Double) row.get(1)));
        double correlation = Statistics.corr(vectors.keys(), vectors.values(), "pearson");
        System.out.println("Pearson index: " + correlation);

        System.out.println("All jobs completed");
        //Scanner input = new Scanner(System.in);
        //input.nextLine();

        jsc.close();
        spark.close();
    }

}
