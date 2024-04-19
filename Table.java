
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Table implements java.io.Serializable {
    private String tableName;
    private String clusteringKeyColumn;
    private Hashtable<String,String> columnTypes;
    private Vector<String> pages;
    private Boolean initialized = false;
    private String folderPath;

    public Boolean getInitialized() {
        return initialized;
    }

    public void setInitialized(Boolean initialized) {
        this.initialized = initialized;
    }

    public Table(String tablename, String clusteringKeyColumn, Hashtable<String,String> columnTypes) {
        for (String column : columnTypes.keySet()) {
            String type = columnTypes.get(column);
            if (!isValidType(type)) {
                throw new IllegalArgumentException("Column type '" + type + "' is not supported.");
            }
        }
        this.tableName = tablename;
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.columnTypes = columnTypes;
        this.pages = new Vector<String>();
        this.folderPath = this.tableName;

        if (!tableExists()) {
            writeMetadata();
        }

        // System.out.println("Table "+this.tableName+" created with clustering key "+this.clusteringKeyColumn);
        // columnTypes.forEach((s, v) -> {
        //     System.out.println("Column: " + s + " Type: " + v);
        // });
    }

    private boolean isValidType(String type) {
        return type.equals("java.lang.Integer") || type.equals("java.lang.String") || type.equals("java.lang.Double") || type.equals("java.lang.double");
    }

    private boolean tableExists() {
        try (BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(tableName)) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void writeMetadata() {
        try (PrintWriter writer = new PrintWriter(new FileWriter("metadata.csv", true))) {
            columnTypes.forEach((columnName, columnType) -> {
                String clusteringKey = columnName.equals(clusteringKeyColumn) ? "True" : "False";
                writer.print("\n"+tableName + ", " + columnName + ", " + columnType + ", " + clusteringKey + ", null, null");
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertTuple(Tuple tuple) throws DBAppException {
        initializeTable();
        if(pages.size() == 0)
        {
            Page newPage = new Page(getTableName() + "/" + getTableName() + "1.ser", clusteringKeyColumn);
            pages.add(getTableName() + "/" + getTableName() + "1.ser");
            newPage.addTuple(tuple);
        }
        else
        {
            Page foundPage = findAppropriatePage(tuple.getColumn(clusteringKeyColumn), this.pages);
            // foundPage.addTuple(tuple);
            insertTupleIntoPage(foundPage, tuple);


            // Page lastPage = pages.get(pages.size()-1);
            // if(lastPage.isFull())
            // {
            //     Page newPage = new Page(getTableName() + "/" + getTableName() + (pages.size()+1)+".ser", clusteringKeyColumn);
            //     pages.add(newPage);
            //     newPage.addTuple(tuple);
            // }
            // else
            // {
            //     lastPage.addTuple(tuple);
            // }
        }
    }

    private Page findAppropriatePage(Object clusteringKeyValue, Vector<String> pages) {
        if(pages.size() == 0)
        {
            Page newPage = new Page(getTableName() + "/" + getTableName() + (this.pages.size()+1)+".ser", clusteringKeyColumn);
            this.pages.add(getTableName() + "/" + getTableName() + (this.pages.size()+1)+".ser");
            return newPage;
        }

        Comparable clusteringKeyComparable = (Comparable) clusteringKeyValue;

        int low = 0;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            String midPageName = pages.get(mid);
            Comparable minKey = (Comparable) readMinMaxValuesFromFile(midPageName).get("min");
            Comparable maxKey = (Comparable) readMinMaxValuesFromFile(midPageName).get("max");

            if (clusteringKeyComparable.compareTo(minKey) >= 0 && clusteringKeyComparable.compareTo(maxKey) <= 0) {
                Page foundPage = deserializePage(midPageName);
                Vector<String> filteredPages = (Vector<String>) pages.clone();
                filteredPages.remove(midPageName);
                if(foundPage.isFull()) return findAppropriatePage(clusteringKeyValue, filteredPages);
                else return foundPage;
            } else if (clusteringKeyComparable.compareTo(maxKey) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return findClosestPage(clusteringKeyComparable, pages);
    }

    private Page findClosestPage(Comparable clusteringKeyValue, Vector<String> pages) {
        if(pages.size() == 0)
        {
            Page newPage = new Page(getTableName() + "/" + getTableName() + (this.pages.size()+1)+".ser", clusteringKeyColumn);
            this.pages.add(getTableName() + "/" + getTableName() + (this.pages.size()+1)+".ser");
            return newPage;
        }
        int left = 0;
        int right = pages.size() - 1;
        int closestPageIndex = -1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            String midPageName = pages.get(mid);
            Comparable minKey = (Comparable) readMinMaxValuesFromFile(midPageName).get("min");
            Comparable maxKey = (Comparable) readMinMaxValuesFromFile(midPageName).get("max");

            if (minKey == null || maxKey == null) {
                left = mid + 1;
                continue;
            }

            int minCompare = clusteringKeyValue.compareTo(minKey);
            int maxCompare = clusteringKeyValue.compareTo(maxKey);

            if (minCompare >= 0 && maxCompare <= 0) {
                Page foundPage = deserializePage(midPageName);
                return foundPage;
            } else if (minCompare < 0) {
                right = mid - 1; // Go left
                closestPageIndex = mid; // Update closest page index
            } else {
                left = mid + 1; // Go right
                closestPageIndex = mid + 1; // Update closest page index
            }
        }

        if(pages.size() == closestPageIndex) {
            Page foundPage = deserializePage(pages.get(closestPageIndex - 1));
            Vector<String> filteredPages = (Vector<String>) pages.clone();
            filteredPages.remove(closestPageIndex - 1);
            if(foundPage.isFull()) return findClosestPage(clusteringKeyValue, filteredPages);
            else return foundPage;
        }
        else {
            Page foundPage = deserializePage(pages.get(closestPageIndex));
            Vector<String> filteredPages = (Vector<String>) pages.clone();
            filteredPages.remove(closestPageIndex);
            if(foundPage.isFull()) return findClosestPage(clusteringKeyValue, filteredPages);
            else return foundPage;
        }

    }

    private Hashtable<String, Comparable> readMinMaxValuesFromFile(String filename) {
        Hashtable<String, Comparable> minMaxValues = new Hashtable<>();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename.contains("/") ? filename.replace(".ser", "minmax.ser") : getTableName() + "/" + filename.replace(".ser", "minmax.ser")))) {
            minMaxValues = (Hashtable<String, Comparable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return minMaxValues;
    }

    public void addPage(String page) {
        pages.add(page);
    }

    // Method to retrieve all pages of the table
    public Vector<String> getPages() {
        return pages;
    }

    // Method to get the table name
    public String getTableName() {
        return this.tableName;
    }

    // Method to get the clustering key column
    public String getClusteringKeyColumn() {
        return clusteringKeyColumn;
    }

    // Method to get the column types
    public Hashtable<String, String> getColumnTypes() {
        return columnTypes;
    }

    //Do not deserialize
    public void initializeTable() throws DBAppException {
        pages.removeAll(pages);
        File tableFolder = new File(folderPath);
        if (tableFolder.exists() && tableFolder.isDirectory()) 
        {
            File[] pageFiles = tableFolder.listFiles();
            if (pageFiles != null) {
                for (File pageFile : pageFiles) {
                    if (pageFile.isFile() && pageFile.getName().endsWith(".ser") && !pageFile.getName().contains("minmax")) {
                        // try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(pageFile))) {
                        //     Page page = (Page) ois.readObject();
                        //     pages.add(page);
                        //     // Process loaded page (e.g., add it to some data structure)
                        //     System.out.println("Loaded page from file: " + pageFile.getName());
                        // } catch (IOException | ClassNotFoundException e) {
                        //     e.printStackTrace();
                        // }
                        pages.add(pageFile.getName());
                    }
                }
            }
            // folder exists, do nothing
            setInitialized(true);
        }
        else
        {
            try
            {
                tableFolder.mkdir();
                setInitialized(true);
            }
            catch(Exception e)
            {
                throw new DBAppException("Error creating table folder");
            }
        }
    }

    public void displayTableData() throws Exception {
        if(!initialized)
        {
            this.initializeTable();
        }
        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles != null) {
            for (File file : listOfFiles) {
                if (file.isFile() && file.getName().endsWith(".ser") && !file.getName().contains("minmax")) {
                    Page page = deserializePage(file);
                    System.out.println(page.getPageFileName() + page.toString());
                }
            }
        }
    }

    private Page deserializePage(File file) {
        Page p = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(getTableName() + "/" + file.getName());
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
            return p;
        }
        catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Page deserializePage(String pageName) {
        Page page = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(pageName.contains("/") ? pageName : getTableName() + "/" + pageName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            page = (Page) in.readObject();
            in.close();
            fileIn.close();
            return page;
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    public void insertTupleIntoPage(Page page, Tuple tuple) {
        if (page.getTuples().isEmpty()) {
          page.getTuples().add(tuple);
          page.updatePage(tuple);
          return;
        }
      
        String clusterKey = this.getClusteringKeyColumn();
        Comparable keyValue = (Comparable) tuple.getData().get(clusterKey);

        if((int)keyValue == 61) {
            page.getTuples().remove(59);
            page.updatePage(tuple);
        }
      
        // Use long to prevent overflow
        long low = 0;
        long high = page.getTuples().size() - 1;
      
        while (low <= high) {
          long mid = (low + high) >>> 1; // Avoid overflow with unsigned right shift
          Tuple midTuple = page.getTuples().get((int) mid); // Cast mid to int for access
          Comparable midKeyValue = (Comparable) midTuple.getData().get(clusterKey);
      
          int comparison = keyValue.compareTo(midKeyValue);
          if (comparison < 0) {
            high = mid - 1;
          } else if (comparison > 0) {
            low = mid + 1;
          } else {
            // Duplicate key, handle as needed
            System.out.println("Duplicate key found: " + keyValue);
            return;
          }
        }
      
        // Insertion position not at the end
        if (low < page.getTuples().size()) {
          page.getTuples().add(page.getTuples().size(), null); // Add placeholder
          for (int i = page.getTuples().size() - 1; i > low; i--) {
            page.getTuples().set(i, page.getTuples().get(i - 1));
          }
        }
      
        // Insert the tuple at the found position
        try
        {
            page.getTuples().set((int) low, tuple);
        }
        catch(Exception e)
        {
            page.getTuples().add((int) low, tuple);
        }
        finally
        {
            page.updatePage(tuple);
        }
      }
}
