/*package classic_relational;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


 *
 *  d:\rogyf\Gitlab\maadb_amazon\Java\resources\products.json d:\rogyf\Gitlab\maadb_amazon\Java\resources\reviews.json jdbc:postgresql://localhost:5432/maadb postgres demo
 *  d:\rogyf\Gitlab\maadb_amazon\Java\resources\products.json d:\rogyf\Gitlab\maadb_amazon\Java\resources\reviews.json jdbc:postgresql://localhost:5432/rmaadb postgres demo
 *

public class Loader {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        DriverDB db = new DriverDB(args[2], args[3], args[4]);
        db.createProducts();
        db.createReviews();

        try {
            System.out.println("Inserimento Products...");
            insertProducts(db, args[0]);
            System.out.println("Inserimento Products Completata");
            System.out.println("-------------------------------------");
            System.out.println("Inserimento Reviews...");
            insertReviews(db, args[1]);
            System.out.println("Inserimento Reviews Completata");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        float sec = (end - start) / 1000F;
        System.out.println("Time elapsed : " + sec + " sec");
    }

    public static void insertProducts(DriverDB db, String path) throws IOException {
        InputStream fileStream = new FileInputStream(path);
        Reader decoder = new InputStreamReader(fileStream, "UTF-8");
        BufferedReader buffered = new BufferedReader(decoder);
        List<Product> cache = new ArrayList<>();
        String myLine;
        while ((myLine = buffered.readLine()) != null) {
            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse(myLine);
                Product product = new Product((String) json.get("asin"), (String) json.get("description"), (String) json.get("title"));
                cache.add(product);

                if (cache.size() >= DriverDB.cacheSize) {
                    db.insertProducts("products", cache);
                    cache.clear();
                    System.out.println("cache flush");
                }
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println(myLine);
            }
        }

        if (cache.size() > 0) {
            db.insertProducts("products", cache);
            cache.clear();
        }
    }

    public static void insertReviews(DriverDB db, String path) throws IOException {
        InputStream fileStream = new FileInputStream(path);
        Reader decoder = new InputStreamReader(fileStream, "UTF-8");
        BufferedReader buffered = new BufferedReader(decoder);
        List<Review> cache = new ArrayList<>();
        String myLine;
        int count = 0;
        while ((myLine = buffered.readLine()) != null) {
            count++;
            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse(myLine);
                Map<String, String> dictOverall = (Map<String, String>) json.get("overall");
                float overall = Float.parseFloat(dictOverall.get("$numberInt"));
                String date = (String) json.get("reviewTime");
                date = date.split(",")[1];
                date = date.trim();
                int time = Integer.parseInt(date);

                Review review = new Review(count, (String) json.get("reviewText"), (String) json.get("asin"), (String) json.get("summary"), overall, time, (String) json.get("reviewerName"));
                cache.add(review);

                if (cache.size() >= DriverDB.cacheSize) {
                    db.insertReviews("reviews", cache);
                    cache.clear();
                    System.out.println("cache flush");
                }
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println(myLine);
            }
        }

        if (cache.size() > 0) {
            db.insertReviews("reviews", cache);
            cache.clear();
        }
    }
}
*/