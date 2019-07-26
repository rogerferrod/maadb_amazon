import java.io.Serializable;

/**
 * Product object
 *
 * @author Simone Cullino
 * @author Roger Ferrod
 * @version 3.11
 */
public class Product implements Serializable {

    private String id;
    private String description;
    private String title;
    private float polarityAVG;


    public Product() {
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

    public float getPolarityAVG() {
        return polarityAVG;
    }

    public void setPolarityAVG(float polarityAVG) {
        this.polarityAVG = polarityAVG;
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
}
