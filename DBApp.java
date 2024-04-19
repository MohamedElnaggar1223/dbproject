
/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.FileInputStream;
import java.io.ObjectInputStream;


public class DBApp {
	private static final String METADATA_FILE = "metadata.csv";
    private Vector<Table> tables;

	public Vector<Table> getTables() {
		return tables;
	}

	public void setTables(Vector<Table> tables) {
		this.tables = tables;
	}

	public DBApp( ){
		this.tables = new Vector<Table>();
		initializeTables();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		
	}

	private void initializeTables() {
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            String currentTableName = null;
            Hashtable<String, String> columnTypes = new Hashtable<>();
            String clusteringKeyColumn = null;
            while ((line = reader.readLine()) != null) {
				System.out.println(line);
                String[] parts = line.split(", ");
                String tableName = parts[0];
                String columnName = parts[1];
                String columnType = parts[2];
                boolean isClusteringKey = Boolean.parseBoolean(parts[3]);
                String indexName = parts[4];
                String indexType = parts[5];

                if (!tableName.equals(currentTableName)) {
                    if (currentTableName != null) {
                        Table table = new Table(currentTableName, clusteringKeyColumn, columnTypes);
                        tables.add(table);
                    }
                    currentTableName = tableName;
                    columnTypes = new Hashtable<>();
                }
                
                columnTypes.put(columnName, columnType);
                if (isClusteringKey) {
                    clusteringKeyColumn = columnName;
                }
            }
            // Add the last table
            if (currentTableName != null) {
				System.out.println(currentTableName);
				System.out.println(clusteringKeyColumn);
                Table table = new Table(currentTableName, clusteringKeyColumn, columnTypes);
                tables.add(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	public void printTables() {
        for (Table table : tables) {
            System.out.println("Table Name: " + table.getTableName());
            System.out.println("Columns:");
            for (String columnName : table.getColumnTypes().keySet()) {
                String columnType = table.getColumnTypes().get(columnName);
                System.out.println("\t" + columnName + ": " + columnType);
            }
            System.out.println();
        }
    }

	public void printTablesContent() throws Exception
	{
		for (Table table : tables) {
			table.displayTableData();
		}
	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	@SuppressWarnings("unchecked")
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
		try
		{
			Table tb = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
			tables.add(tb);
			// Page page = new Page("Student.ser");
			// tb.addPage(page);
			// Hashtable htblColNameValue = new Hashtable();
			// htblColNameValue.put("id", new Integer( 453455 ));
			// htblColNameValue.put("name", new String("Ahmed Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.95 ) );
			// page.addTuple(htblColNameValue);
			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer( 453456 ));
			// htblColNameValue.put("name", new String("Najajer El Fajer" ) );
			// htblColNameValue.put("gpa", new Double( 1.95 ) );
			// page.addTuple(htblColNameValue);
			// System.out.println(page.toString());
		}
		catch(Exception e)
		{
			throw e;
		}
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		throw new DBAppException("not implemented yet");
	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
	
		try
		{
			Table table = findTableByName(strTableName);
			if (table == null) {
				throw new DBAppException("Table '" + strTableName + "' does not exist.");
			}
			Tuple insertedTuple = new Tuple();
			insertedTuple.setData(htblColNameValue);
			table.insertTuple(insertedTuple);
		}
		catch(Exception e)
		{
				throw e;
		}
	}

	private Table findTableByName(String tableName) {
        for (Table table : tables) {
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
								try{
								// 	Table table = findTableByName(strTableName);
								// 	if(table == null){
								// 		throw new DBAppException("Table '"+strTableName+"' does not exist");
								// 	}								
								// 	String mypage = table.getPages().get(0);
								// 	int j = 0;
								// 	while(j<table.getPages().size()){
								// 	mypage = table.getPages().get(j);
								// 	Vector<Tuple> tuple = mypage.getTuples();

								// 	for(int i = 0;i<tuple.size();i++){
								// 		Tuple curr = (Tuple)tuple.get(i);
								// 		Hashtable<String, Object> currData = curr.getData();
								// 		if(currData.containsValue(Integer.parseInt(strClusteringKeyValue))){
								// 			String repKey = htblColNameValue.keySet().toArray()[0].toString();
								// 			currData.put(repKey, htblColNameValue.get(repKey));										
								// 			int index = tuple.indexOf(curr);		
								// 			tuple.set(index, curr);	
								// 			mypage.saverforpub();
								// 			break;
								// 		}
								// 	}
								// j++;
								//	}
						}
								catch(Exception e){
									throw new DBAppException(e.getMessage());
								}
	
		
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main( String[] args ){
	
	try
	{
		String strTableName = "Gogo";
		DBApp dbApp = new DBApp();
		
		System.out.println("About to print tables--------------------------------------------------------------------------!");
		dbApp.printTables();
		System.out.println("Table printed!");

		// Hashtable htblColNameType = new Hashtable(); 
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
		// dbApp.createTable( strTableName, "id", htblColNameType );
		// // dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

		// Hashtable htblColNameValue = new Hashtable( );
		// htblColNameValue.put("id", new Integer( 0 ));
		// htblColNameValue.put("address", new String("Test test address" ) );
		// htblColNameValue.put("gogo", new String("testttttt" ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// Hashtable newme = new Hashtable();

		// newme.put("id",new Integer(1));
		// newme.put("address", new String("This is my address"));
		// newme.put("gogo", "gogovalue");

		// dbApp.insertIntoTable(strTableName, newme);

		// Hashtable newme2 = new Hashtable();

		// newme2.put("id",new Integer(2));
		// newme2.put("address", new String("This is my address"));
		// newme2.put("gogo", "gogovalue");
		
		// dbApp.insertIntoTable(strTableName, newme2);

		// Hashtable newval = new Hashtable();

		// newval.put("id", new Integer(208));
		// newval.put("address", new String("naggars Address"));
		// newval.put("gogo", new String("what is gogo"));


		


			
		//insertion into gogo table 	
			

		for(int k = 1 ; k<250;k++){
			Hashtable newhash = new Hashtable();

			newhash.put("id", new Integer(k+3));
			newhash.put("address", new String("Yassin Foudas Address"));
			newhash.put("gogo", new String("what is gogo"));

			dbApp.insertIntoTable(strTableName, newhash);

		}

		//insertion into Student table	

		for (int k = 0; k < 205; k++) {
			Hashtable newhash = new Hashtable();

			newhash.put("id", new Integer(k + 3));
			newhash.put("name", new String("Yassin Mohamed Fouda"));
			newhash.put("gpa", new Double(1.88));

			dbApp.insertIntoTable("Student", newhash);
		}

		// dbApp.insertIntoTable(strTableName, newval);

		dbApp.tables.forEach(table -> {
			try
			{
				table.displayTableData();
			}
			catch(Exception e)
			{

			}
		});

		//new values for gogo table

			// Hashtable trial = new Hashtable();
			// trial.put("address", new String("Yassins new address"));

			// Hashtable value2 = new Hashtable();
			// value2.put("gogo", "i have been replaced :(");

		//new value for student table

			// Hashtable stud1 = new Hashtable();
			// stud1.put("address", new String("Najajer address"));

		

			// String strSecTableName = "Gogo";
			// htblColNameType.clear();
			// htblColNameType.put("id", "java.lang.Integer");
			// htblColNameType.put("address", "java.lang.String");
			// htblColNameType.put("gogo", "java.lang.double");
			// dbApp.createTable( strSecTableName, "id", htblColNameType );
			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(1234));
			// htblColNameValue.put("address", new String("Gogo land is so fun"));
			// htblColNameValue.put("gogo", new Double(123.45));
			// dbApp.insertIntoTable(strSecTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5678));
			// htblColNameValue.put("address", new String("Gogo land is so fun x2"));
			// htblColNameValue.put("gogo", new Double(567.89));
			// dbApp.insertIntoTable(strSecTableName, htblColNameValue);

			//dbApp.printTables();
			
			
			// dbApp.getTables().get(0).displayTableData();

			//updating gogo table with new values

			// dbApp.updateTable(strTableName, "2", trial);
			// dbApp.updateTable(strTableName, "103",value2);

			// //updating Student table with new value

			// dbApp.updateTable("Gogo", "207", stud1);


			// dbApp.printTablesContent();

		/* 	FileInputStream filein = new FileInputStream("Gogo/Gogo1.ser") ;

			ObjectInputStream in = new ObjectInputStream(filein);

			Page mypage = (Page) in.readObject();

			in.close();

			filein.close();*/

		
			


			

			// htblColNameValue.clear( );

			// htblColNameValue.put("id", new Integer( 453455 ));
			// htblColNameValue.put("name", new String("Ahmed Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.95 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 5674567 ));
			// htblColNameValue.put("name", new String("Dalia Noor" ) );
			// htblColNameValue.put("gpa", new Double( 1.25 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 23498 ));
			// htblColNameValue.put("name", new String("John Noor" ) );
			// htblColNameValue.put("gpa", new Double( 1.5 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// htblColNameValue.clear( );
			// htblColNameValue.put("id", new Integer( 78452 ));

			// htblColNameValue.put("name", new String("Zaky Noor" ) );
			// htblColNameValue.put("gpa", new Double( 0.88 ) );
			// dbApp.insertIntoTable( strTableName , htblColNameValue );

			// SQLTerm[] arrSQLTerms;
			// arrSQLTerms = new SQLTerm[2];
			// arrSQLTerms[0]._strTableName =  "Student";
			// arrSQLTerms[0]._strColumnName=  "name";
			// arrSQLTerms[0]._strOperator  =  "=";
			// arrSQLTerms[0]._objValue     =  "John Noor";

			// arrSQLTerms[1]._strTableName =  "Student";
			// arrSQLTerms[1]._strColumnName=  "gpa";
			// arrSQLTerms[1]._strOperator  =  "=";
			// arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			// String[]strarrOperators = new String[1];
			// strarrOperators[0] = "OR";
			// // select * from Student where name = "John Noor" or gpa = 1.5;
			// Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}


	



}