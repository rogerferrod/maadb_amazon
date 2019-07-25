public class Review {

    private int id;
    private String reviewerName;
    private String reviewText;
    private String asin;
    private String title;
    private Integer reviewTime;
    private Float polarity;
    private float overall;

    public Review(int id, String reviewText, String asin, String title, float overall, int reviewTime, String reviewerName) {
        this.id = id;
        this.reviewerName = reviewerName;
        this.reviewText = reviewText;
        this.asin = asin;
        this.title = title;
        this.reviewTime = reviewTime;
        this.polarity = null;
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

    public Integer getReviewTime() {
        return reviewTime;
    }

    public Float getPolarity() {
        return polarity;
    }

    public float getOverall() {
        return overall;
    }

    public void setPolarity(Float polarity) {
        this.polarity = polarity;
    }

    public void setReviewText(String reviewText) {
        this.reviewText = reviewText;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setReviewerName(String reviewerName) {
        this.reviewerName = reviewerName;
    }

    public boolean hasReviewerName() {
        return reviewerName != null;
    }

    public boolean hasReviewText() {
        return reviewText != null;
    }

    public boolean hasTitle() {
        return title != null;
    }

    public boolean hasPolarity() {
        return polarity != null;
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
