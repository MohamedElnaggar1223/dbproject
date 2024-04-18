import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Vector;
 
public class Page implements java.io.Serializable {
    private Vector<Tuple> tuples;
    private String filename;
    private String clusteringKeyColumn;
    private Comparable minValue;
    private Comparable maxValue;
    
    public Page(String filename, String clusteringKeyColumn) {
        tuples = new Vector<>();
        this.filename = filename;
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.minValue = null;
        this.maxValue = null;
    }

    public void addTuple(Tuple tuple) {
        tuples.add(tuple);
        updateMinMax(tuple);
        saveToFile();
    }

    public String getPageFileName() {
        return this.filename;
    }

    private void updateMinMax(Tuple tuple) {
        Comparable clusteringKeyValue = (Comparable) tuple.getColumn(clusteringKeyColumn);
        if (minValue == null || clusteringKeyValue.compareTo(minValue) < 0) {
            minValue = clusteringKeyValue;
        }
        if (maxValue == null || clusteringKeyValue.compareTo(maxValue) > 0) {
            maxValue = clusteringKeyValue;
        }
    }
    
    public Vector<Tuple> getTuples() {
        return tuples;
    }

    private void saveToFile() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(this);
            System.out.println("Page saved to file: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String modifiedFilename =  new String(filename).replace(".ser", "minmax.ser");

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(modifiedFilename))) {
            Hashtable<String, Comparable> minMaxObj = new Hashtable<>();
            minMaxObj.put("min", minValue);
            minMaxObj.put("max", maxValue);
            oos.writeObject(minMaxObj);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isFull()
    {
        return tuples.size() >= 200;
    }

    // Override toString() method to return the tuples in the page in one string separated by commas
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Object tuple : tuples) {
            sb.append(tuple.toString());
        }
        return sb.toString();
    }

    public void saverforpub(){
        saveToFile();
    }
}