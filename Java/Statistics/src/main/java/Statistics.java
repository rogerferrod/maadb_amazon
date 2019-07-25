import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

import java.util.*;

public final class Statistics {

    public static void main(final String[] args) throws InterruptedException {
        if (args.length < 1) {
            throw new RuntimeException("Missing parameters");
        }

        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .appName("Statistics")
                .config("spark.mongodb.input.uri", "mongodb://" + args[0] + "/amazon.reviews")
                .config("spark.mongodb.output.uri", "mongodb://" + args[0] + "/amazon.statistics")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // More application logic would go here...
        JavaMongoRDD<Document> reviews_rdd = MongoSpark.load(jsc);

        // Analyze data from MongoDB
        System.out.print("Numero recensioni: " + reviews_rdd.count());


        JavaPairRDD<String, Integer> statistics = reviews_rdd.flatMapToPair(review -> {
            List<Tuple2<String, Integer>> couples = new ArrayList<>();
            Double stars = (Double) review.get("overall");
            String text = (String) review.get("reviewText");
            String year = (String) review.get("reviewTime");
            if (year != null) {
                year = year.split(",", 2)[1];
                year = year.trim();
            }


            Pattern pattern = Pattern.compile("^(http:\\/\\/www\\.|https:\\/\\/www\\.|http:\\/\\/|https:\\/\\/)?[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?(\\/.*)?$");
            if (text != null) {
                Matcher matcher = pattern.matcher(text);
                if (matcher.find()) {
                    couples.add(new Tuple2<>("url", 1));
                }
            }

            if (year != null) {
                couples.add(new Tuple2<>("year", Integer.parseInt(year)));
            }
            if (stars != null) {
                couples.add(new Tuple2<>("stars", stars.intValue()));
            }
            return couples.iterator();
        });

        Map<String, Long> map = statistics.countByKey();
        for (String key : map.keySet()) {
            System.out.println("Occorrenze " + key + ": " + map.get(key));
        }


        JavaPairRDD<String, Double> textRDD = reviews_rdd.flatMapToPair(review -> {
            List<Tuple2<String, Double>> couples = new ArrayList<>();
            String text = (String) review.get("reviewText");
            String title = (String) review.get("summary");

            if (text != null) {
                couples.add(new Tuple2<>("text", (double) text.length()));
                String textWord[] = text.split(" ");
                couples.add(new Tuple2<>("text_words", (double) textWord.length));
            }
            if (title != null) {
                couples.add(new Tuple2<>("title", (double) title.length()));
                String titleWord[] = title.split(" ");
                couples.add(new Tuple2<>("title_words", (double) titleWord.length));
            }
            return couples.iterator();
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

        JavaPairRDD<String, Tuple2<Double, Integer>> avgCounts =
                textRDD.combineByKey(createCombiner, mergeValue, mergeCombiners);


        for (Tuple2<String, Tuple2<Double, Integer>> string : avgCounts.collect()) {
            System.out.println("Occorrenze " + string._1 + ": " + string._2._2());
            System.out.println("Lunghezza media " + string._1 + ": " + string._2._1() / string._2._2());
        }

        //Scanner input = new Scanner(System.in);
        //input.nextLine();
        jsc.close();

    }
}