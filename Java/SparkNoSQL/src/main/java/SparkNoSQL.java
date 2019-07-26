import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import scala.Tuple2;


import org.apache.spark.mllib.stat.Statistics;

import java.io.IOException;
import java.util.*;

/**
 * NoSQL approach
 * MongoDB / Spark - Cluster
 *
 * @author Simone Cullino
 * @author Roger Ferrod
 * @version 3.11
 */
public class SparkNoSQL {

    /**
     * Applies VADER and computes Pearson index
     * Reads data from MongoDB cluster and writes results in new collections
     *
     * @param args[0] MongoDB router ip address
     * @param args[1] lexicon.txt file path
     * @param args[2] emoticon.txt file path
     * @param args[3] slang.txt file path
     * @param args[4] modifier.txt file path
     * @param args[5] negate.txt file path
     */
    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            throw new RuntimeException("Missing parameters");
        }

        // Create the SparkSession.
        System.out.println("Initialize project");
        SparkSession spark = SparkSession.builder()
                .appName("SparkNoSQL")
                //.master("local")
                .config("spark.mongodb.input.uri", "mongodb://" + args[0] + "/amazon.reviews")
                .config("spark.mongodb.output.uri", "mongodb://" + args[0] + "/amazon.statistics")
                .getOrCreate();

        // Create JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Initialize Broadcast variables
        final Broadcast<Map<String, Float>> lexicon = jsc.broadcast(Vader.makeLexiconDictionary(args[1]));
        final Broadcast<Map<String, Float>> emoticon = jsc.broadcast(Vader.makeEmoticonDictionary(args[2]));
        final Broadcast<Map<String, String>> slang = jsc.broadcast(Vader.makeSlangDictionary(args[3]));
        final Broadcast<Map<String, Float>> modifier = jsc.broadcast(Vader.makeModifierDictionary(args[4]));
        final Broadcast<ArrayList<String>> negate = jsc.broadcast(Vader.makeNegateList(args[5]));

        // Application logic
        Map<String, String> readOverrides = new HashMap<>();
        readOverrides.put("collection", "reviews");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // reviewsRDD <Document>
        JavaMongoRDD<Document> reviewsRDD = MongoSpark.load(jsc, readConfig);

        //JavaRDD<Document> sample = reviewsRDD.sample(false, 0.01);
        //System.out.println("Samples: " + sample.count());
        JavaRDD<Document> reviews_score = reviewsRDD.map(review -> {
            //JavaRDD<Document> reviews_score = sample.map(review -> {
            try {
                String reviewText = (String) review.get("reviewText");
                String reviewTitle = (String) review.get("summary");

                if (reviewText.length() > 0 && reviewTitle.length() > 0) {

                    List<String> sentences = Vader.tokenizeSentence(reviewText);
                    Vader vaderText = new Vader(lexicon.value(), emoticon.value(), slang.value(), modifier.value(), negate.value(), sentences);
                    Vader vaderTitle = new Vader(lexicon.value(), emoticon.value(), slang.value(), modifier.value(), negate.value(), reviewTitle);
                    float textScore = vaderText.evaluate();
                    float titleScore = vaderTitle.evaluate();
                    float tot = (textScore + titleScore) / 2;

                    review.put("score", tot);
                } else {
                    review.put("score", null);
                }
            } catch (Exception e) {
                System.out.println("error " + e);
            }
            return review;
        });

        reviews_score = reviews_score.filter(review -> review.get("score") != null);
        reviews_score.persist(StorageLevel.MEMORY_AND_DISK()); //caching

        // Write results
        System.out.println("Writing scores...");
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "scores");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        MongoSpark.save(reviews_score, writeConfig);

        // Pearson index
        System.out.println("Computing Pearson index...");
        // vectors <stars, score>
        JavaPairRDD<Double, Double> vectors = reviews_score.mapToPair(review -> {
            Double stars = (Double) review.get("overall");
            double score = normalize((Float) review.get("score"));

            return new Tuple2<>(stars, score);
        });

        double correlation = Statistics.corr(vectors.keys(), vectors.values(), "pearson");
        System.out.println("Pearson index: " + correlation);


        // Join
        System.out.println("Computing Join...");

        Map<String, String> readProdOverrides = new HashMap<>();
        readProdOverrides.put("collection", "products");
        readProdOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readProdConfig = ReadConfig.create(jsc).withOptions(readProdOverrides);

        // productsRDD <Product>
        JavaMongoRDD<Document> productsRDD = MongoSpark.load(jsc, readProdConfig);

        // products <asin, Product>
        JavaPairRDD<String, Document> products = productsRDD.mapToPair(product -> {
            String id = (String) product.get("asin");
            return new Tuple2<>(id, product);
        });

        // scores <asin, score>
        JavaPairRDD<String, Double> scores = reviews_score.mapToPair(review -> {
            String id = (String) review.get("asin");
            double score = (Float) review.get("score");
            return new Tuple2<>(id, score);
        });

        /*
         * map (value, 1) for each key in partition
         * Input: (k,v)
         * Output: (k, (value, 1))
         */
        Function<Double, Tuple2<Double, Integer>> createCombiner =
                x -> new Tuple2<>(x, 1);

        /*
         * combiner (for each partition)
         * Input: (k, (value, 1))
         * Output: (k, (total, count))
         */
        Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>> mergeValue =
                (Tuple2<Double, Integer> t, Double y) -> new Tuple2<>(t._1() + y, t._2() + 1);

        /*
         * merge across partition
         * Input: (k, (total, count))
         * Output: (k, (total_all_partitions, count_all_partitions))
         */
        Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> mergeCombiners =
                (Tuple2<Double, Integer> x, Tuple2<Double, Integer> y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2());

        // averages <asin, (tot, count)>
        JavaPairRDD<String, Tuple2<Double, Integer>> averages =
                scores.combineByKey(createCombiner, mergeValue, mergeCombiners);

        // scoreAvg <asin, average>
        JavaPairRDD<String, Double> scoreAvg = averages.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._1 / tuple._2._2));

        // joined <asin, (Product, average)>
        JavaPairRDD<String, Tuple2<Document, Double>> joined = products.join(scoreAvg);

        // results <Product>
        JavaRDD<Document> results = joined.map(tuple -> {
            Document product = tuple._2._1;
            product.put("score_average", tuple._2._2);
            return product;
        });

        // Write results
        System.out.println("Writing products...");
        Map<String, String> writeProdOverrides = new HashMap<>();
        writeProdOverrides.put("collection", "products_score");
        writeProdOverrides.put("writeConcern.w", "majority");
        WriteConfig writeProdConfig = WriteConfig.create(jsc).withOptions(writeProdOverrides);

        MongoSpark.save(results, writeProdConfig);

        System.out.println("All jobs completed");
        //Scanner input = new Scanner(System.in);
        //input.nextLine();

        jsc.close();
        spark.close();
    }

    private static double normalize(double x) {
        return ((x - (-1)) / (1 - (-1))) * (5 - 1) + 1;
    }

}
