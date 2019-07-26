import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;


/**
 * Relational approach
 * JDBC only - Centralized server
 *
 * @author Simone Cullino
 * @author Roger Ferrod
 * @version 3.11
 */
public class Relational {

    /**
     * Applies VADER and computes Pearson index
     * Reads data from Relational DBMS and writes results in new tables
     *
     * @param args[0] lexicon.txt file path
     * @param args[1] emoticon.txt file path
     * @param args[2] slang.txt file path
     * @param args[3] modifier.txt file path
     * @param args[4] negate.txt file path
     * @param args[5] JDBC url (jdbc:postgresql://localhost:5432/rmaadb)
     * @param args[6] DB user
     * @param args[7] DB password
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Loading...");
        long start = System.currentTimeMillis();

        DriverDB db = new DriverDB(args[5], args[6], args[7]);
        db.createReviewScore();
        db.createProductScore();

        List<Float> x = new ArrayList<>();
        List<Float> y = new ArrayList<>();
        List<Review> reviewsCache = new ArrayList<>();
        List<Product> productsCache = new ArrayList<>();
        Map<Product, Tuple2<Float, Integer>> avgProduct = new HashMap<>(); //product:<num_review , sum_score>

        // Initialize variables
        Map<String, Float> lexicon = Vader.makeLexiconDictionary(args[0]);
        Map<String, Float> emoticon = Vader.makeEmoticonDictionary(args[1]);
        Map<String, String> slang = Vader.makeSlangDictionary(args[2]);
        Map<String, Float> modifier = Vader.makeModifierDictionary(args[3]);
        ArrayList<String> negate = Vader.makeNegateList(args[4]);

        // Application logic
        int sizeTableReviews = db.sizeTable("reviews");
        int limit = DriverDB.cacheSize;
        for (int offset = 0; offset < sizeTableReviews; offset += limit) {
            System.out.println("from " + offset + " to " + (offset + limit));
            Map<Product, ArrayList<Review>> groups = db.getReviewsGroupByProductsOffsetLimit(limit, offset);
            List<Product> products = new ArrayList<>(groups.keySet());

            for (Product p : products) {
                ArrayList<Review> reviews = groups.get(p);
                float scoreCount = 0;
                for (Review review : reviews) {
                    try {
                        String text = review.getReviewText();
                        String title = review.getTitle();
                        float overall = review.getOverall();
                        if (text.length() > 0 && title.length() > 0) {
                            List<String> sentences = Vader.tokenizeSentence(text);
                            Vader vaderText = new Vader(lexicon, emoticon, slang, modifier, negate, sentences);
                            Vader vaderTitle = new Vader(lexicon, emoticon, slang, modifier, negate, title);
                            float textScore = vaderText.evaluate();
                            float titleScore = vaderTitle.evaluate();
                            float tot = (textScore + titleScore) / 2;
                            review.setPolarity(tot);
                            x.add(overall);
                            y.add(tot);
                            scoreCount = scoreCount + tot;
                        } // DEFAULT is null
                    } catch (IOException e) {
                        System.out.println(e.toString());
                    }
                }

                List<Review> filtered = reviews.stream()
                        .filter(rev -> rev.getPolarity() != null)
                        .collect(Collectors.toList());

                reviewsCache.addAll(filtered);

                if (reviewsCache.size() >= DriverDB.cacheSize) {
                    System.out.println("review cache flush");
                    db.insertReviews("reviews_score", reviewsCache);
                    reviewsCache.clear();
                }
                if (avgProduct.containsKey(p)) {
                    Tuple2<Float, Integer> temp = avgProduct.get(p);
                    avgProduct.put(p, new Tuple2<>(temp._1 + scoreCount, temp._2 + filtered.size()));
                    p.setPolarityAVG((temp._1 + scoreCount) / (temp._2 + filtered.size()));
                    productsCache.remove(p);
                    productsCache.add(p);
                } else {
                    avgProduct.put(p, new Tuple2<>(scoreCount, filtered.size()));
                    p.setPolarityAVG(scoreCount / filtered.size());
                    if (p.getPolarityAVG() != null) {
                        productsCache.add(p);
                    }
                }
            }

            if (productsCache.size() > DriverDB.cacheSize) {
                System.out.println("product cache flush");
                db.insertProducts("products_score", productsCache);
                productsCache.clear();
            }
        }

        if (reviewsCache.size() > 0) {
            System.out.println("review cache flush");
            db.insertReviews("reviews_score", reviewsCache);
            reviewsCache.clear();
        }

        if (productsCache.size() > 0) {
            System.out.println("product cache flush");
            db.insertProducts("products_score", productsCache);
            productsCache.clear();
        }

        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println("Time elapsed : " + sec);

        System.out.println("Computing Pearson...");
        double[] XX = Arrays.stream(x.toArray())
                .mapToDouble(num -> Double.parseDouble(num.toString()))
                .toArray();
        double[] YY = Arrays.stream(y.toArray())
                .mapToDouble(num -> Double.parseDouble(num.toString()))
                .toArray();
        PearsonsCorrelation pearson = new PearsonsCorrelation();
        double correlation = pearson.correlation(XX, YY);
        System.out.println("Pearson = " + correlation);
    }

}
