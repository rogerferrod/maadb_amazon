import java.io.Serializable;

public class Review implements Serializable {
    private int id;
    private String reviewerName;
    private String reviewText;
    private String asin;
    private String title;
    private int reviewTime;
    private Double polarity;
    private Double overall;

    public Review() {

    }

    public void setId(int id) {
        this.id = id;
    }

    public void setReviewerName(String reviewerName) {
        this.reviewerName = reviewerName;
    }

    public void setReviewText(String reviewText) {
        this.reviewText = reviewText;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setReviewTime(int reviewTime) {
        this.reviewTime = reviewTime;
    }

    public void setPolarity(Double polarity) {
        this.polarity = polarity;
    }

    public void setOverall(Double overall) {
        this.overall = overall;
    }

    public int getId() {
        return id;
    }

    public String getReviewerName() {
        return reviewerName;
    }

    public String getReviewText() {
        return reviewText;
    }

    public String getAsin() {
        return asin;
    }

    public String getTitle() {
        return title;
    }

    public int getReviewTime() {
        return reviewTime;
    }

    public Double getPolarity() {
        return polarity;
    }

    public Double getOverall() {
        return overall;
    }

    @Override
    public String toString() {
        return "Review{" +
                "id=" + id +
                ", reviewerName='" + reviewerName + '\'' +
                ", reviewText='" + reviewText + '\'' +
                ", asin='" + asin + '\'' +
                ", title='" + title + '\'' +
                ", reviewTime=" + reviewTime +
                ", polarity=" + polarity +
                ", overall=" + overall +
                '}';
    }
}
