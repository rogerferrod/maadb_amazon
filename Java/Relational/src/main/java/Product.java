import java.util.Objects;

public class Product {

    private String id;
    private String description;
    private String title;
    private Float polarityAVG;

    public Product(String id, String description, String title) {
        this.id = id;
        this.description = description;
        this.title = title;
        this.polarityAVG = null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Float getPolarityAVG() {
        return polarityAVG;
    }

    public void setPolarityAVG(float polarityAVG) {
        this.polarityAVG = polarityAVG;
    }

    public boolean hasDescription() {
        return description != null;
    }

    public boolean hasTitle() {
        return title != null;
    }

    public boolean hasPolarityAVG(){
        return polarityAVG != null;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id='" + id + '\'' +
                ", description='" + description + '\'' +
                ", title='" + title + '\'' +
                ", polarityAVG=" + polarityAVG +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return Objects.equals(id, product.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id);
    }
}
