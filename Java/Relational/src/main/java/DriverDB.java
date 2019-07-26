import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DB Driver
 * Provides methods to interact with DB
 *
 * @author Simone Cullino
 * @author Roger Ferrod
 * @version 3.11
 */
public class DriverDB {

    static final int cacheSize = 2000;

    private String url;
    private String user;
    private String psw;

    public DriverDB(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.psw = password;
    }

    public void createReviewScore() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        try {
            st.executeUpdate("CREATE TABLE reviews_score" +
                    "(ID INTEGER PRIMARY KEY, " +
                    "ReviewerName VARCHAR(50), " +
                    "ReviewText TEXT, " +
                    "Title TEXT," +
                    "ReviewTime INTEGER, " +
                    "Asin VARCHAR(20)," +
                    "Overall FLOAT," +
                    "Polarity FLOAT," +
                    "FOREIGN KEY (Asin) REFERENCES Products (ID))");
            System.out.println("Tabella ReviewsScore creata correttamente");
            st.execute("CREATE INDEX asin_index ON reviews USING BTREE (asin)");
            System.out.println("Indice BTREE Reviews creato correttamente");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void createProductScore() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        try {
            st.executeUpdate("CREATE TABLE products_score" +
                    "(ID VARCHAR(20) PRIMARY KEY, " +
                    "Title TEXT, " +
                    "Description TEXT," +
                    "PolarityAVG FLOAT)");
            System.out.println("Tabella ProductsScore creata correttamente");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void createReviews() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        try {
            st.executeUpdate("CREATE TABLE reviews" +
                    "(ID INTEGER PRIMARY KEY, " +
                    "ReviewerName VARCHAR(50), " +
                    "ReviewText TEXT, " +
                    "Title TEXT," +
                    "ReviewTime INTEGER, " +
                    "Asin VARCHAR(20)," +
                    "Overall FLOAT," +
                    "Polarity FLOAT," +
                    "FOREIGN KEY (Asin) REFERENCES Products (ID))");
            System.out.println("Tabella Reviews creata correttamente");
            st.execute("CREATE INDEX asin_index ON reviews USING BTREE (asin)");
            System.out.println("Indice BTREE Reviews creato correttamente");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void createProducts() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        try {
            st.executeUpdate("CREATE TABLE products" +
                    "(ID VARCHAR(20) PRIMARY KEY, " +
                    "Title TEXT, " +
                    "Description TEXT," +
                    "PolarityAVG FLOAT)");
            System.out.println("Tabella Products creata correttamente");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void insertReviews(String table, List<Review> reviews) {
        Connection conn = null;
        Statement st = null;
        StringBuilder sql = new StringBuilder("INSERT INTO " + table + " VALUES ");
        for (Review review : reviews) {
            if (review.hasReviewText()) {
                review.setReviewText(review.getReviewText().replace("'", "''"));
            }
            if (review.hasTitle()) {
                review.setTitle(review.getTitle().replace("'", "''"));
            }
            if (review.hasReviewerName()) {
                review.setReviewerName(review.getReviewerName().replace("'", "''"));
            }
            sql.append("('").append(review.getId()).append("', '").append(review.getReviewerName()).append("', '").append(review.getReviewText()).append("', '").append(review.getTitle()).append("', '").append(review.getReviewTime()).append("', '").append(review.getAsin()).append("', '").append(review.getOverall()).append("', ");
            if (review.hasPolarity()) {
                sql.append("'").append(review.getPolarity()).append("'),");
            } else {
                sql.append("null),");
            }
        }

        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
            st.executeUpdate(sql.toString());
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void insertProducts(String table, List<Product> products) {
        Connection conn = null;
        Statement st = null;
        StringBuilder sql = new StringBuilder("INSERT INTO " + table + " VALUES ");
        for (Product product : products) {
            if (product.hasDescription()) {
                product.setDescription(product.getDescription().replace("'", "''"));
            }
            if (product.hasTitle()) {
                product.setTitle(product.getTitle().replace("'", "''"));
            }
            sql.append("('").append(product.getId()).append("', '").append(product.getTitle()).append("', '").append(product.getDescription()).append("', ");
            if (product.hasPolarityAVG()) {
                sql.append("'").append(product.getPolarityAVG()).append("'),");
            } else {
                sql.append("null),");
            }
        }

        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        sql.append("ON CONFLICT (id) DO UPDATE \n" +
                "  SET polarityavg = excluded.polarityavg");

        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
            st.executeUpdate(sql.toString());
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<Product, ArrayList<Review>> getReviewsGroupByProductsOffsetLimit(int limit, int offset) {
        Connection conn = null;
        Statement st = null;
        Map<Product, ArrayList<Review>> result = new HashMap<>();
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT p.id as asin, p.title as prodTitle, p.description as description, r.id as id, r.title as title, r.reviewtext as text, r.overall as overall, r.reviewtime as time, r.reviewername as name\n" +
                    "FROM  (\n" +
                    "   SELECT *\n" +
                    "   FROM   reviews\n" +
                    "   ORDER  BY asin\n" +
                    "   LIMIT  " + limit + "\n" +
                    "   OFFSET " + offset + "\n" +
                    "   ) as r\n" +
                    "JOIN products as p ON p.id = r.asin");
            ArrayList<Review> temp;
            while (rs.next()) {
                Product p = new Product(rs.getString("asin"), rs.getString("description"), rs.getString("prodTitle"));
                if (result.containsKey(p)) {
                    temp = result.get(p);
                } else {
                    temp = new ArrayList<>();
                    result.put(p, temp);
                }

                Review review = new Review(rs.getInt("id"), rs.getString("text"), rs.getString("asin"), rs.getString("title"), rs.getFloat("overall"), rs.getInt("time"), rs.getString("name"));
                temp.add(review);
            }
            rs.close();
            st.close();
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public int sizeTable(String table_name) {
        Connection conn = null;
        Statement st = null;
        int result = -1;
        try {
            conn = DriverManager.getConnection(url, user, psw);
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("select count(*) from " + table_name);
            while (rs.next()) {
                result = rs.getInt("count");
            }
            rs.close();
            st.close();
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

}
