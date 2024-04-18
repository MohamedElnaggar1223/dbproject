import java.util.*;

public class Tuple implements java.io.Serializable {
    private Hashtable<String, Object> data;

    public Tuple() {
        data = new Hashtable<String, Object>();
    }

    public void setColumn(String column, Object value) {
        data.put(column, value);
    }

    public Object getColumn(String column) {
        return data.get(column);
    }

    public boolean containsColumnName(String column) {
        return data.containsKey(column);
    }
    
    public void setData(Hashtable<String, Object> data) {
        this.data = data;
    }

    public Hashtable<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        // Iterate over the values and append them to the StringBuilder
        Iterator<Object> iterator = this.data.values().iterator();
        while (iterator.hasNext()) {
            result.append(iterator.next());
            if (iterator.hasNext()) {
                result.append(", "); // Add comma and space if it's not the last element
            }
        }

        return result.toString()+ "\n";
    }
}
