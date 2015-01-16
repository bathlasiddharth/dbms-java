import java.awt.List;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MyDatabase {
	
	//characters list
	static String dbPath="";
	static String dbName="data.db";
	static String idIndexPath="";
	static String idIndexName="id.ndx";
	static String lastNameIndexPath="";
	static String lastNameIndexName="last_name.ndx";
	static String stateIndexPath="";
	static String stateIndexName="state.ndx";
	static String emailIndexPath="";
	static String emailIndexName="email.ndx";
	static String splitFieldChar = ",";
	static String dbFieldChar = "^";
	static String dbLineChar = "|";
	
	
	
	
	
	
	
	
	
	
	//...............................Comparator...............................
	
	
		//Compare 2 ID indexes in indexArrayList
		static Comparator<ArrayList> idIndexComparator = new Comparator<ArrayList>() {
			public int compare(ArrayList a, ArrayList b){
				Integer int1=Integer.parseInt((String) a.get(0));
				Integer int2=Integer.parseInt((String) b.get(0));
				return int1.compareTo(int2);
			}
		};
		
		static Comparator<ArrayList> textIndexComparator = new Comparator<ArrayList>() {
			public int compare(ArrayList a, ArrayList b){
				String int1=(String) a.get(0);
				String int2=(String) b.get(0);
				return int1.compareTo(int2);
			}
		};
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	
	//...............................get arrayList from index files...............................
	
	
	//read id ArrayList object from id.ndx
	private static ArrayList<ArrayList> getIdArrayListFromIndexFile() throws FileNotFoundException, IOException, ClassNotFoundException{
		
		ObjectInputStream indexFile = new ObjectInputStream(new FileInputStream(idIndexPath+idIndexName));
		ArrayList<ArrayList> idIndexArray=(ArrayList<ArrayList>) indexFile.readObject();
		indexFile.close();
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(idIndexPath+idIndexName));
		o.writeObject(idIndexArray);
		return idIndexArray;
	}
	
	//read lastName ArrayList object from last_name.ndx
	private static ArrayList<ArrayList> getLastNameArrayListFromIndexFile() throws FileNotFoundException, EOFException, IOException, ClassNotFoundException{
		
		ObjectInputStream indexFile = new ObjectInputStream(new FileInputStream(lastNameIndexPath+lastNameIndexName));
		ArrayList<ArrayList> lastNameIndexArray= new ArrayList();
		lastNameIndexArray=(ArrayList<ArrayList>) indexFile.readObject();
		indexFile.close();
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(lastNameIndexPath+lastNameIndexName));
		o.writeObject(lastNameIndexArray);
		return lastNameIndexArray;
	}
	
	//read state ArrayList object from state.ndx
	private static ArrayList<ArrayList> getStateArrayListFromIndexFile() throws FileNotFoundException, IOException, ClassNotFoundException{
		
		ObjectInputStream indexFile = new ObjectInputStream(new FileInputStream("state.ndx"));
		ArrayList<ArrayList> stateIndexArray=new ArrayList();
		stateIndexArray=(ArrayList<ArrayList>) indexFile.readObject();
		indexFile.close();
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(stateIndexPath+stateIndexName));
		o.writeObject(stateIndexArray);
		
		return stateIndexArray;
	}
	
	//read email ArrayList object from email.ndx
	private static ArrayList<ArrayList> getEmailArrayListFromIndexFile() throws FileNotFoundException, IOException, ClassNotFoundException{
		
		ObjectInputStream indexFile = new ObjectInputStream(new FileInputStream("email.ndx"));
		ArrayList<ArrayList> emailIndexArray=new ArrayList();
		emailIndexArray=(ArrayList<ArrayList>) indexFile.readObject();
		indexFile.close();
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(emailIndexPath+emailIndexName));
		o.writeObject(emailIndexArray);
		
		return emailIndexArray;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................insert data from csv file to data.db file...............................
	
	
	//add records from a CSV file to data.db
	public static void fileToDatabase(String Path, String fileName) throws IOException, ClassNotFoundException{
		BufferedReader br = null;
		String line="";
		
		br = new BufferedReader(new FileReader(Path+fileName));
		line=br.readLine();
		int i=1;
		while((line = br.readLine()) != null){
			System.out.println("\nRecord "+i+" from CSV:");
			i++;
			addRecordToDb(line, true);
		}
		br.close();
	}
	
	//Add a record to data.db
	public static void addRecordToDb(String record, boolean print) throws IOException, ClassNotFoundException{
		RandomAccessFile dbFile = new RandomAccessFile(dbPath+dbName, "rw");
		dbFile.seek(dbFile.length());
		
		long recordStartPos=dbFile.length();
		
		record=replaceComma(record);//replace comma in the text so that a line of csv can be split without error
		record=record.replace("\"", "");//remove double quotes from csv text
		
		String[] fileData = record.split(splitFieldChar);
		//check if ID exists or not
		if(doesIndexExists(Integer.parseInt(fileData[0]),fileData[11])==-1){
			int k=0;
			addIdIndex(record, recordStartPos);
			addLastNameIndex(record, recordStartPos);
			addStateIndex(record, recordStartPos);
			addEmailIndex(record, recordStartPos);
			for(String s:fileData){
				if(k==0){
					int a=Integer.parseInt(s);
					dbFile.writeChar(a);
					dbFile.writeChars(dbFieldChar);
				}
				else{
					s.replace("~",",");
					dbFile.writeChars(s);
					dbFile.writeChars(dbFieldChar);
				}
				k++;
			}
			
			dbFile.writeChars(dbLineChar);
			if(print==true)
				System.out.println("Record inserted successfully");
		}
		else if(doesIndexExists(Integer.parseInt(fileData[0]),fileData[11])==1) {
			if(print==true)
				System.out.println("Record with this ID (" +fileData[0]+") already exists");
		}
		else if(doesIndexExists(Integer.parseInt(fileData[0]),fileData[11])!=-1){
			if(print==true)
				System.out.println("Record with this Email (" +fileData[11]+") already exists");
		}
		dbFile.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................check if index already exists...............................
	
	
	//check if an ID index already exists
	public static int doesIndexExists(int index, String email) throws FileNotFoundException, ClassNotFoundException, IOException{
		
		ArrayList<ArrayList> idIndexArray = getIdArrayListFromIndexFile();
		
		ArrayList<Integer> idList=new ArrayList();
		
		for(ArrayList i:idIndexArray){
			idList.add(Integer.parseInt((String) i.get(0)));
		}
		
		ArrayList<ArrayList> emailIndexArray = getEmailArrayListFromIndexFile();
		
		ArrayList<String> emailList=new ArrayList();
		
		for(ArrayList i:emailIndexArray){
			emailList.add((String) i.get(0));
		}
		
		if(idList.contains(index)){
			return 1;
		}else if(emailList.contains(email)){
			return 2;
		}
		else{
			return -1;
		}
			
	}
	
	
	//check if a state index already exists
	public static int doesLastNameExists(String lastName) throws FileNotFoundException, ClassNotFoundException, IOException{
		
		ArrayList<ArrayList> lastNameIndexArray = getLastNameArrayListFromIndexFile();
		
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(lastNameIndexPath+lastNameIndexName));
		o.writeObject(lastNameIndexArray);
		
		ArrayList<String> lastNameList=new ArrayList();
		
		for(ArrayList i:lastNameIndexArray){
			lastNameList.add((String) i.get(0));
		}
		
		if(lastNameList.contains(lastName)){
			return lastNameList.indexOf(lastName);
		}
		else{
			return -1;
		}
	}
			
		
		
		
	//check if a state index already exists
	public static int doesStateExists(String state) throws FileNotFoundException, ClassNotFoundException, IOException{
		
		ArrayList<ArrayList> stateIndexArray = getStateArrayListFromIndexFile();
		//System.out.println(stateIndexArray.toString());
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(stateIndexPath+stateIndexName));
		o.writeObject(stateIndexArray);
		
		ArrayList<String> stateList=new ArrayList();
		
		for(ArrayList i:stateIndexArray){
			stateList.add((String) i.get(0));
		}
		
		if(stateList.contains(state)){
			return stateList.indexOf(state);
		}else{
			return -1;
		}
	}
	
	//check if email index already exists
	public static int doesEmailExists(String email) throws FileNotFoundException, ClassNotFoundException, IOException{
		
		ArrayList<ArrayList> emailIndexArray = getEmailArrayListFromIndexFile();
		//System.out.println(emailIndexArray.toString());
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(emailIndexPath+emailIndexName));
		o.writeObject(emailIndexArray);
		
		ArrayList<String> emailList=new ArrayList();
		
		for(ArrayList i:emailIndexArray){
			emailList.add((String) i.get(0));
		}
		
		if(emailList.contains(email)){
			return emailList.indexOf(email);
		}else{
			return -1;
		}
	}
		
	
	


	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................add data to index files...............................
	
	
	//add record's ID to ID index file
	private static void addIdIndex(String record, long position) throws IOException, ClassNotFoundException{
	    
		record = replaceComma(record);
		record=record.replace("\"", "");
		ArrayList<ArrayList> idIndexArray=getIdArrayListFromIndexFile();
				
		ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(idIndexPath+idIndexName));
		
		String[] recordData = record.split(splitFieldChar);
		
		ArrayList idArray=new ArrayList();
		idArray.add(recordData[0]);
		idArray.add(position);
		//System.out.println(idArray.get(1));
		idIndexArray.add(idArray);
		
		Collections.sort(idIndexArray, idIndexComparator);
		
		indexFileOutput.writeObject(idIndexArray);
		
		indexFileOutput.flush();
		indexFileOutput.close();
	}
	
	//add record's last name to last_name index file
	private static void addLastNameIndex(String record, long position) throws IOException, ClassNotFoundException{
	    
		record = replaceComma(record);
		record=record.replace("\"", "");
		ArrayList<ArrayList> lastNameIndexArray=getLastNameArrayListFromIndexFile();
				
		
		String[] recordData = record.split(splitFieldChar);
		String lastName=recordData[2];
		
		if(doesLastNameExists(lastName)>=0){
			int pos=doesLastNameExists(lastName);
			ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(lastNameIndexPath+lastNameIndexName));
			lastNameIndexArray.get(pos).add(position);
			indexFileOutput.writeObject(lastNameIndexArray);
			
			indexFileOutput.flush();
			indexFileOutput.close();
		}
		else{
			ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(lastNameIndexPath+lastNameIndexName));
			ArrayList lastNameArray=new ArrayList();
			lastNameArray.add(lastName);
			lastNameArray.add(position);
			//System.out.println(idArray.get(1));
			lastNameIndexArray.add(lastNameArray);
			Collections.sort(lastNameIndexArray, textIndexComparator);
			//System.out.println(lastName);
			//System.out.println(lastNameIndexArray.size());
			indexFileOutput.writeObject(lastNameIndexArray);
					
			indexFileOutput.flush();
			indexFileOutput.close();
		}
		
	}
		
		
	
	//add record's state to state index file
	private static void addStateIndex(String record, long position) throws IOException, ClassNotFoundException{
	    
		record = replaceComma(record);
		record=record.replace("\"", "");
		ArrayList<ArrayList> stateIndexArray= new ArrayList();
		stateIndexArray=getStateArrayListFromIndexFile();
		
		String[] recordData = record.split(splitFieldChar);
		String state=recordData[7];
		
		if(doesStateExists(state)>=0){
			int pos=doesStateExists(state);
			ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(stateIndexPath+stateIndexName));
			stateIndexArray.get(pos).add(position);
			indexFileOutput.writeObject(stateIndexArray);
			
			indexFileOutput.flush();
			indexFileOutput.close();
		}
		else{
			ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(stateIndexPath+stateIndexName));
			//System.out.println("hi");
			ArrayList stateArray=new ArrayList();
			stateArray.add(state);
			stateArray.add(position);
			//System.out.println(idArray.get(1));
			stateIndexArray.add(stateArray);
			Collections.sort(stateIndexArray, textIndexComparator);
			//System.out.println(state);
			//System.out.println(stateIndexArray.size());
			indexFileOutput.writeObject(stateIndexArray);
					
			indexFileOutput.flush();
			indexFileOutput.close();
		}
		
	}
	
	//add record's email to email index file
	private static void addEmailIndex(String record, long position) throws IOException, ClassNotFoundException{
	    
		record = replaceComma(record);
		record=record.replace("\"", "");
		ArrayList<ArrayList> emailIndexArray=getEmailArrayListFromIndexFile();
				
		ObjectOutputStream indexFileOutput = new ObjectOutputStream(new FileOutputStream(emailIndexPath+emailIndexName));
		
		String[] recordData = record.split(splitFieldChar);
		
		ArrayList emailArray=new ArrayList();
		emailArray.add(recordData[11]);
		emailArray.add(position);
		//System.out.println(emailArray.get(1));
		emailIndexArray.add(emailArray);
		
		Collections.sort(emailIndexArray, textIndexComparator);
		
		indexFileOutput.writeObject(emailIndexArray);
		
		indexFileOutput.flush();
		indexFileOutput.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................fixed functions...............................
	
	//this function replaces 'comma' in a CSV line by ~
	private static String replaceComma(String data) {
        String replacedData = data;
        Pattern p = Pattern.compile("\"[a-zA-Z0-9 ]+[,][ a-zA-Z0-9]+\"");
        Matcher matcher = p.matcher(replacedData);
       
        while(matcher.find()){
           String replacement = matcher.group().replaceAll(",", "~");
           replacedData = replacedData.replaceAll( matcher.group(), replacement);
        }
       
        return replacedData;
    }
	
	//run this function to create default index and db files
	public static void setup() throws IOException, ClassNotFoundException, EOFException{
		
		File fileTemp = new File("data.db");
        if (fileTemp.exists()){
           fileTemp.delete();
        }
        fileTemp = new File("id.ndx");
        if (fileTemp.exists()){
           fileTemp.delete();
        }
        fileTemp = new File("last_name.ndx");
        if (fileTemp.exists()){
           fileTemp.delete();
        }
        fileTemp = new File("state.ndx");
        if (fileTemp.exists()){
           fileTemp.delete();
        }
        fileTemp = new File("email.ndx");
        if (fileTemp.exists()){
           fileTemp.delete();
        }
		
		
		ArrayList<ArrayList> idIndexArray = new ArrayList();
		ObjectOutputStream idIndexFile = new ObjectOutputStream(new FileOutputStream("id.ndx"));
		idIndexFile.writeObject(idIndexArray);
		
		ArrayList<ArrayList> lastNameIndexArray = new ArrayList();
		ObjectOutputStream lastNameIndexFile = new ObjectOutputStream(new FileOutputStream("last_name.ndx"));
		lastNameIndexFile.writeObject(lastNameIndexArray);
		
		ArrayList<ArrayList> stateIndexArray = new ArrayList();
		ObjectOutputStream stateIndexFile = new ObjectOutputStream(new FileOutputStream("state.ndx"));
		stateIndexFile.writeObject(stateIndexArray);
		
		ArrayList<ArrayList> emailIndexArray = new ArrayList();
		ObjectOutputStream emailIndexFile = new ObjectOutputStream(new FileOutputStream("email.ndx"));
		emailIndexFile.writeObject(emailIndexArray);
		
		idIndexFile.close();
		lastNameIndexFile.close();
		stateIndexFile.close();
	}
	
	
	
	
	
	
	
	 
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................actions...............................
	
	//return record at a particular position in data.db
	public static String RecordAtPosition(long pos) throws IOException{
		RandomAccessFile raf=new RandomAccessFile(dbPath+dbName, "r");
		raf.seek(pos);
		String record="";
		int id=raf.readUnsignedShort();
		char c= Character.forDigit(id,10);
		record=record+id;
		while(c!=dbLineChar.charAt(0)){
			c=raf.readChar();
			record=record+c;
		}
		raf.close();
		return record;
	}
	
	//print columns for viewing operations output
	public static void printColumns(){
		System.out.printf("%-5s %-20s %-20s %-35s %-40s %-35s %-15s %-5s %-10s %-20s %-20s %-40s %-40s %n", "ID", "First Name", "Last Name", "Company Name", "Address", "City", "County", "State", "Zip", "Phone1", "Phone2", "Email", "Web");
	}
	
	//print record in tabular format
	public static void printRecord(String record){
		String[] recordData=record.split("\\^");
		for(int i=0;i<recordData.length;i++){
			recordData[i]=recordData[i].replace("~", ",");
		}
		System.out.printf("%-5s %-20s %-20s %-35s %-40s %-35s %-15s %-5s %-10s %-20s %-20s %-40s %-40s %n", recordData[0], recordData[1], recordData[2], recordData[3], recordData[4], recordData[5], recordData[6], recordData[7], recordData[8], recordData[9], recordData[10], recordData[11], recordData[12]);
		//System.out.println(record);
	}
	
	//print all records
	public static void printAllRecords() throws FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> arrList=getIdArrayListFromIndexFile();
		System.out.println("All Records: ("+arrList.size()+" records)");
		printColumns();
		for(ArrayList i:arrList){
			String record=RecordAtPosition((long) i.get(1));
			printRecord(record);
		}
	}
	
	//SELECT command
	public static String selectAction(String field, String value, boolean print) throws FileNotFoundException, ClassNotFoundException, IOException{
		String record="";
		if(field=="id"){
			ArrayList<ArrayList> idArrayList=new ArrayList();
			idArrayList=getIdArrayListFromIndexFile();
			
			ArrayList idArray=new ArrayList();
			for(ArrayList i:idArrayList){
				idArray.add(i.get(0));
			}
			if(idArray.contains(value)){
				if(print==true)
					System.out.println("Record Details where ID="+value+": (1 Record(s))");
				int pos=idArray.indexOf(value);
				long offset=(long) idArrayList.get(pos).get(1);
				//System.out.println(offset);
				record=RecordAtPosition(offset);
				if(print==true)
					printColumns();
				if(print==true)
					printRecord(record);
			}
			else{
				if(print==true)
					System.out.println("There is no record where id="+value);
			}
		}
		
		else if(field=="state"){
			
			if(doesStateExists(value)!=-1){
				int pos=doesStateExists(value);
				ArrayList<ArrayList> stateArrayList=new ArrayList();
				stateArrayList=getStateArrayListFromIndexFile();
				if(print==true)
					System.out.println("Record Details where State="+value+": ("+(stateArrayList.get(pos).size()-1)+" record(s))");
				if(print==true)
					printColumns();
				for(int i=1;i<stateArrayList.get(pos).size();i++){
					long offset=(long) stateArrayList.get(pos).get(i);
					record=RecordAtPosition(offset);
					if(print==true)
						printRecord(record);
				}
			}
			else{
				if(print==true)
					System.out.println("There is no record where state="+value);
			}
						/*ArrayList<ArrayList> stateArrayList=new ArrayList();
						stateArrayList=getStateArrayListFromIndexFile();
						ArrayList stateArray=new ArrayList();
						for(ArrayList i:stateArrayList){
							stateArray.add(i.get(0));
						}
						if(stateArray.contains(value)){
							System.out.println("Record Details where State="+value+":");
							int pos=stateArray.indexOf(value);
							for(int i=1;i<stateArrayList.get(pos).size();i++){
								long offset=(long) stateArrayList.get(pos).get(i);
								String record=RecordAtPosition(offset);
								printColumns();
								printRecord(record);
							}
						}*/
		}
		else if(field=="last_name"){
			
			if(doesLastNameExists(value)!=-1){
				int pos=doesLastNameExists(value);
				ArrayList<ArrayList> lastNameArrayList=new ArrayList();
				lastNameArrayList=getLastNameArrayListFromIndexFile();
				if(print==true)
					System.out.println("Record Details where Last Name="+value+": ("+(lastNameArrayList.get(pos).size()-1)+" record(s))");
				printColumns();
				for(int i=1;i<lastNameArrayList.get(pos).size();i++){
					long offset=(long) lastNameArrayList.get(pos).get(i);
					record=RecordAtPosition(offset);
					if(print==true)
						printRecord(record);
				}
			}
			else{
				if(print==true)
					System.out.println("There is no record where lastName="+value);
			}			
		}
		else if(field=="email"){
			
			if(doesEmailExists(value)!=-1){
				int pos=doesEmailExists(value);
				ArrayList<ArrayList> emailArrayList=new ArrayList();
				emailArrayList=getEmailArrayListFromIndexFile();
				if(print==true)
					System.out.println("Record Details where Last Name="+value+": ("+(emailArrayList.get(pos).size()-1)+" record(s))");
				if(print==true)
					printColumns();
				for(int i=1;i<emailArrayList.get(pos).size();i++){
					long offset=(long) emailArrayList.get(pos).get(i);
					record=RecordAtPosition(offset);
					if(print==true)
						printRecord(record);
				}
			}
			else{
				if(print==true)
					System.out.println("There is no record where email="+value);
			}			
		}
		
		else if(field=="*" && value=="*"){
			if(print==true)
				printAllRecords();
		}
		else{
			if(print==true)
				System.out.println("Wrong parameters");
		}
		return record;
	}
	
	
	
	
	
	//INSERT command
	public static void insertAction(String record) throws ClassNotFoundException, IOException{
		record=record.replace("','", "|");
		record=record.replace(",", "~");
		record=record.replace("|", ",");
		record=record.replace("'", "");
		record=record.replace("\"", "");
		addRecordToDb(record, true);
	}
	
	//DELETE command
	public static void deleteAction(String field, String value, Boolean print) throws FileNotFoundException, ClassNotFoundException, IOException{
		
		if(field=="id"){
			ArrayList<ArrayList> idArrayList=new ArrayList();
			idArrayList=getIdArrayListFromIndexFile();
			
			ArrayList idArray=new ArrayList();
			for(ArrayList i:idArrayList){
				idArray.add(i.get(0));
			}
			
			if(idArray.contains(value)){
				int pos=idArray.indexOf(value);
				
				if(print==true)
					System.out.println("Following record(s) deleted: ("+(idArrayList.get(pos).size()-1)+") record(s)");
				String record=deleteRecordAtPosition((long) idArrayList.get(pos).get(1));
				deleteIdIndex(record);
				deleteLastNameIndex(record, (long) idArrayList.get(pos).get(1));
				deleteStateIndex(record, (long) idArrayList.get(pos).get(1));
				deleteEmailIndex(record, (long) idArrayList.get(pos).get(1));
				if(print==true)
					printColumns();
				if(print==true)
					printRecord(record);
			}
			else{
				if(print==true)
					System.out.println("There is no record where ID="+value);
			}
		}
		else if(field=="last_name"){
			if(doesLastNameExists(value)>=0){
				int pos=doesLastNameExists(value);
				ArrayList<ArrayList> arrList=getLastNameArrayListFromIndexFile();
				if(print==true)
					System.out.println("Following Record(s) deleted: ("+(arrList.get(pos).size()-1)+") record(s)");
				printColumns();
				
				for(int i=1;i<arrList.get(pos).size();i++){
					String record=deleteRecordAtPosition((Long) arrList.get(pos).get(i));
					deleteIdIndex(record);
					deleteLastNameIndex(record, (Long) arrList.get(pos).get(i));
					deleteEmailIndex(record, (Long) arrList.get(pos).get(i));
					if(print==true)
						printRecord(record);
				}
				
				
				
			}
			else{
				if(print==true)
					System.out.println("There is no record where lastName="+value);
			}
		}
		else if(field=="state"){
			if(doesStateExists(value)>=0){
				int pos=doesStateExists(value);
				ArrayList<ArrayList> arrList=getStateArrayListFromIndexFile();
				if(print==true)
					System.out.println("Following Record(s) deleted: ("+(arrList.get(pos).size()-1)+") record(s)");
				if(print==true)
					printColumns();
				
				for(int i=1;i<arrList.get(pos).size();i++){
					String record=deleteRecordAtPosition((Long) arrList.get(pos).get(i));
					deleteIdIndex(record);
					deleteLastNameIndex(record, (Long) arrList.get(pos).get(i));
					deleteStateIndex(record, (Long) arrList.get(pos).get(i));
					deleteEmailIndex(record, (Long) arrList.get(pos).get(i));
					if(print==true)
						printRecord(record);
				}
				
				
				
			}
			else{
				System.out.println("There is no record where state="+value);
			}
		}
		else if(field=="email"){
			ArrayList<ArrayList> emailArrayList=new ArrayList();
			emailArrayList=getEmailArrayListFromIndexFile();
			
			ArrayList emailArray=new ArrayList();
			for(ArrayList i:emailArrayList){
				emailArray.add(i.get(0));
			}
			
			if(emailArray.contains(value)){
				int pos=emailArray.indexOf(value);
				if(print==true)
					System.out.println("Following record(s) deleted: ("+(emailArrayList.get(pos).size()-1)+") record(s)");
				String record=deleteRecordAtPosition((long) emailArrayList.get(pos).get(1));
				deleteIdIndex(record);
				deleteLastNameIndex(record, (long) emailArrayList.get(pos).get(1));
				deleteStateIndex(record, (long) emailArrayList.get(pos).get(1));
				deleteEmailIndex(record, (long) emailArrayList.get(pos).get(1));
				if(print==true)
					printColumns();
				if(print==true)
					printRecord(record);
			}
			else{
				if(print==true)
					System.out.println("There is no record where email="+value);
			}
		}
		else if(field=="*" && value=="*"){
			deleteAllRecords();
		}
		else{
			if(print==true)
				System.out.println("Wrong parameters");
		}
	}
	
	
	//MODIFY command
	public static void modifyAction(String id, String field, String value) throws FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> arrList = getIdArrayListFromIndexFile();
		ArrayList arr=new ArrayList();
		String record="";
		
		for(ArrayList i:arrList){
			arr.add(i.get(0));
		}
		
		if(arr.contains(id)){
			record=selectAction("id", id, false);
			//System.out.println(record);
			deleteAction("id", id, false);
			String[] recordData=record.split("\\^");
			
			ArrayList fieldArray=new ArrayList(Arrays.asList("id", "first_name", "last_name", "company_name", "address", "city", "county", "state", "zip", "phone1", "phone2", "email", "web"));
			if(fieldArray.contains(field)){
				recordData[fieldArray.indexOf(field)]=value;
				record="";
				for(int i=0;i<recordData.length-2;i++){
					recordData[i]=recordData[i].replace("\\,", "\\~");
					record=record+recordData[i];
					record=record+",";
				}
				record=record+recordData[recordData.length-2];
				addRecordToDb(record, false);
				System.out.println("Record has been modified. The new record is:");
				selectAction("id", id, true);
			}
			else{
				System.out.println("You have entered incorrect field:"+field);
			}

		}
		else{
			System.out.println("There is no record where ID="+id);
		}
	}
	
	//COUNT command
	public static int countAction() throws FileNotFoundException, ClassNotFoundException, IOException{
		
		ArrayList<ArrayList> arr=getIdArrayListFromIndexFile();
		return arr.size();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//Delete record starting at particular position in data.db
	public static String deleteRecordAtPosition(Long pos) throws IOException, ClassNotFoundException{
		//long pos=Long.parseLong(position);
		RandomAccessFile raf=new RandomAccessFile(dbPath+dbName, "rw");
		String record="";
		raf.seek(pos);
		int id=raf.readUnsignedShort();
		char c= Character.forDigit(id,10);
		record=record+id;
		while(c!=dbLineChar.charAt(0)){
			c=raf.readChar();
			record=record+c;
		}
		long end=raf.getFilePointer();
		raf.seek(pos);
		while(raf.getFilePointer()<end-2){
			raf.write("0".getBytes());
		}
		raf.close();
		return record;
	}
	
	//remove ID from ID Index File
	private static void deleteIdIndex(String record) throws IOException, ClassNotFoundException{
		//String record=RecordAtPosition(pos);
		String recordData[]=record.split("\\^");
		ArrayList<ArrayList> arrList = new ArrayList<ArrayList>();
		arrList=getIdArrayListFromIndexFile();
		ArrayList arr=new ArrayList();
		for(ArrayList i:arrList){
			arr.add(i.get(0));
		}
		arrList.remove(arr.indexOf(recordData[0]));
		ObjectOutputStream o=new ObjectOutputStream(new FileOutputStream(idIndexPath+idIndexName));
		o.writeObject(arrList);
		o.flush();
		o.close();
		
	}
	
	//remove Last Name from Last Name Index File
	private static void deleteLastNameIndex(String record, long pos) throws FileNotFoundException, EOFException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> arrList = new ArrayList<ArrayList>();
		ArrayList arr=new ArrayList();
		String recordData[]=record.split("\\^");
		arrList=getLastNameArrayListFromIndexFile();
		for(ArrayList i:arrList){
			arr.add(i.get(0));
		}
		//System.out.println(recordData[7]);
		ArrayList a=arrList.remove(arr.indexOf(recordData[2]));
		a.remove(a.indexOf(pos));
			
		if(a.size()>1){
			arrList.add(a);
			Collections.sort(arrList, textIndexComparator);
		}
		ObjectOutputStream o=new ObjectOutputStream(new FileOutputStream(lastNameIndexPath+lastNameIndexName));
		o.writeObject(arrList);
		o.flush();
		o.close();
	}
	
	//remove State from State Index File
	private static void deleteStateIndex(String record, long pos) throws FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> arrList = new ArrayList<ArrayList>();
		ArrayList arr=new ArrayList();
		String recordData[]=record.split("\\^");
		arrList=getStateArrayListFromIndexFile();
		for(ArrayList i:arrList){
			arr.add(i.get(0));
		}
		//System.out.println(recordData[7]);
		ArrayList a=arrList.remove(arr.indexOf(recordData[7]));
		a.remove(a.indexOf(pos));
			
		if(a.size()>1){
			arrList.add(a);
			Collections.sort(arrList, textIndexComparator);
		}
		ObjectOutputStream o=new ObjectOutputStream(new FileOutputStream(stateIndexPath+stateIndexName));
		o.writeObject(arrList);
		o.flush();
		o.close();
	}
	
	//remove Email from Email Index File
	private static void deleteEmailIndex(String record, long pos) throws FileNotFoundException, ClassNotFoundException, IOException{
		String recordData[]=record.split("\\^");
		ArrayList<ArrayList> arrList = new ArrayList<ArrayList>();
		arrList=getEmailArrayListFromIndexFile();
		ArrayList arr=new ArrayList();
		for(ArrayList i:arrList){
			arr.add(i.get(0));
		}
		arrList.remove(arr.indexOf(recordData[11]));
		ObjectOutputStream o=new ObjectOutputStream(new FileOutputStream(emailIndexPath+emailIndexName));
		o.writeObject(arrList);
		o.flush();
		o.close();
		
	}
	
	//delete all records
	private static void deleteAllRecords() throws FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> arrList=getIdArrayListFromIndexFile();
		System.out.println("Following Records deleted: ("+arrList.size()+" records)");
		printColumns();
		for(ArrayList i:arrList){
			String record=RecordAtPosition((long) i.get(1));
			deleteAction("id", (String) i.get(0),true);
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//...............................test code...............................
	
	//print all indexes
	public static void printIndexList() throws FileNotFoundException, ClassNotFoundException, IOException{
		ArrayList<ArrayList> s = new ArrayList<ArrayList>();
		s=getIdArrayListFromIndexFile();
		System.out.println(s.toString());
		s=getLastNameArrayListFromIndexFile();
		System.out.println(s.toString());
		s=getStateArrayListFromIndexFile();
		System.out.println(s.toString());
		s=getEmailArrayListFromIndexFile();
		System.out.println(s.toString());
		
	}
	
	//test the code
	public static void test() throws FileNotFoundException, IOException, ClassNotFoundException{
		/*ObjectInputStream indexFile1 = new ObjectInputStream(new FileInputStream(idIndexPath+idIndexName));
		ArrayList<ArrayList> idIndexArray1=(ArrayList<ArrayList>) indexFile1.readObject();
		System.out.println(idIndexArray1.get(499));
		System.out.println(idIndexArray1.size());
		System.out.println(doesIndexExists(500));
		
		//addRecordToDb("550,Chauncey,Motley,Affiliated With Travelodge,63 E Aurora Dr,Orlando,Orange,FL,32804,407-413-4842,407-557-8857,chauncey_motley@aol.com,http://www.affiliatedwithtravelodge.com");
		indexFile1.close();
		
		//for (int i=0; i<idIndexArray1.size();i++){
		//	System.out.println(idIndexArray1.get(i));
		//}
		*/
		/*
		ArrayList<ArrayList> stateIndexArray = new ArrayList<ArrayList>();
		
		stateIndexArray=getStateArrayListFromIndexFile();
		System.out.println(stateIndexArray.toString());
		
		ObjectOutputStream o=new ObjectOutputStream(new FileOutputStream("state.ndx"));
		o.writeObject(stateIndexArray);
		o.close();*/
		
		
		/*ArrayList<ArrayList> s = new ArrayList<ArrayList>();
		
		s=getEmailArrayListFromIndexFile();
		System.out.println(s.size());
		addRecordToDb("555,Chauncey,Motley,Affiliated With Travelodge,63 E Aurora Dr,Orlando,Orange,FL,32804,407-413-4842,407-557-8857,chauncey1_motley@aol.com,http://www.affiliatedwithtravelodge.com");
		System.out.println(s.toString());
		
		for(ArrayList a:s){
			System.out.println(a.get(0)+" "+a.size());
		}
		*/
		//String r="\"'50211','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','CA','70116','504-621-8927','504-845-1427','jb1utt1@g2mail.com','http://www.bentonjohnbjr.com'\"";
		//insertAction(r);
		ArrayList<ArrayList> s = new ArrayList<ArrayList>();
		
		s=getStateArrayListFromIndexFile();
		//System.out.println(s.size());
		//System.out.println(s.toString());
		//printIndexList();
		//selectAction("email", "http://www.chanayjeffreyaesq.com");
		//selectAction("id", "1", true);
		//selectAction("last_name", "Ferencz");
		//selectAction("state", "AK");
		//selectAction("email", "erick.ferencz@aol.com");
		//selectAction("state", "OH");
		//selectAction("last_name", "Foller");
		//selectAction("email", "sage_wieser@cox.net");
		//deleteAction("id", "4");
		//deleteAction("state", "OH");
		//deleteAction("last_name", "Flosi");
		//deleteAction("state", "AK");
		//deleteAction("emaial", "erick.ferencz@aol.com");
		//deleteIndex((long) 0);s = new ArrayList<ArrayList>();
		//printIndexList();
		//System.out.println(s.toString());
		//printAllRecords();
		//deleteAllRecords();
		//modifyAction("44","id","1");
		System.out.println(countAction());
		//printAllRecords();
	}
	
	//main
	public static void main(String[] args) throws IOException, ClassNotFoundException{
		setup();
		System.out.println("............................................................................................................");
		System.out.println("INSERT DATA FROM CSV");
		System.out.println("............................................................................................................");
		
		fileToDatabase("","us-500.csv");
		
		System.out.println("............................................................................................................");
		System.out.println("INSERT DATA FROM CSV FINISHED");
		System.out.println("............................................................................................................");
		
		//test();
		
		System.out.println("............................................................................................................");
		System.out.println("\n\n\n\n\nINITIAL RECORD COUNT");
		System.out.println("............................................................................................................");
		
		System.out.println("\n Total number of records="+countAction());
		
		System.out.println("\n\n\n\n\n");
		
		System.out.println("............................................................................................................");
		System.out.println("1) SELECT STATEMENTS");
		System.out.println("............................................................................................................");
		System.out.println("\n        1.1 Select record where id=5\n");
		selectAction("id", "5", true);
		
		System.out.println();
		
		System.out.println("\n        1.2 Select record where last_name=Slusarski\n");
		selectAction("last_name", "Slusarski", true);
		
		System.out.println();
		
		System.out.println("\n        1.3 Select record where state=CA\n");
		selectAction("state", "CA", true);
		
		System.out.println();
		
		System.out.println("\n        1.4 Select record where id=5000 (It does not exist)\n");
		selectAction("id", "5000", true);
		
		System.out.println();
		
		System.out.println("\n        1.5 Select record where last_name=Slusarskiaa (It does not exist)\n");
		selectAction("last_name", "Slusarskiaa", true);
		
		System.out.println();
		
		System.out.println("\n        1.6 Select record where state=AA (It does not exist)\n\n");
		selectAction("state", "AA", true);
		
		
		
		System.out.println("............................................................................................................");
		System.out.println("2) INSERT STATEMENTS");
		System.out.println("............................................................................................................");
		System.out.println("\n        2.1 Insert this record: '502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt@gmail.com','http://www.bentonjohnbjr.com'\n");
		insertAction("'502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt12345@gmail.com','http://www.bentonjohnbjr.com'");
		
		System.out.println();
		
		System.out.println("\n        2.2 Insert this record (insert previous record again): '502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt@gmail.com','http://www.bentonjohnbjr.com'\n");
		insertAction("'502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt12345@gmail.com','http://www.bentonjohnbjr.com'");

		System.out.println();
		
		System.out.println("\n        2.3 Insert this record (insert previous record with different id but same email) : '501','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt@gmail.com','http://www.bentonjohnbjr.com'\n");
		insertAction("'501','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt12345@gmail.com','http://www.bentonjohnbjr.com'");

		System.out.println();
		
		System.out.println("\n        2.4 Insert this record (insert previous record with different id and different email) : '502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt1234567@gmail.com','http://www.bentonjohnbjr.com'\n\n");
		insertAction("'501','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt1234567@gmail.com','http://www.bentonjohnbjr.com'");
		
		System.out.println("\n\n Total number of records="+countAction());
		
		
		System.out.println("\n\n        Record with ID=501:");
	    selectAction("id", "501", true);
	    
	    System.out.println("\n\n        Record with ID=502:");
	    selectAction("id", "502", true);
	    
	    
		
		
		
		
		
		System.out.println("\n\n\n\n\n");
		
		
		System.out.println("............................................................................................................");
		System.out.println("3) COUNT STATEMENT");
		System.out.println("............................................................................................................");
		
		System.out.println("\n        Total number of records="+countAction());
		
		System.out.println("\n\n\n\n\n");
		
		
		
		System.out.println("............................................................................................................");
		System.out.println("4) MODIFY STATEMENTS");
		System.out.println("............................................................................................................");
		
		System.out.println("\n        4.1 Modify record where id=5; change first_name to 'ABCDE'\n");
		modifyAction("5", "first_name", "ABCDE");

		System.out.println();
		
		System.out.println("\n        4.2 Modify record where id=5; change last_name to 'PQRST'\n");
		modifyAction("5", "last_name", "PQRST");

		System.out.println();
		
		
		System.out.println("\n        4.3 Modify record where id=5; change company_name to 'A'\n");
		modifyAction("5", "company_name", "A");

		System.out.println();
		
		System.out.println("\n        4.4 Modify record where id=5; change address to 'B'\n");
		modifyAction("5", "address", "B");
		
		System.out.println();
		
		System.out.println("\n        4.5 Modify record where id=5; change city to 'C'\n");
		modifyAction("5", "city", "C");

		System.out.println();
		
		System.out.println("\n        4.6 Modify record where id=5; change county to 'D'\n");
		modifyAction("5", "county", "D");

		System.out.println();
		
		System.out.println("\n        4.6 Modify record where id=5; change state to 'CA'\n");
		modifyAction("5", "state", "CA");

		System.out.println();
		
		System.out.println("\n        4.7 Modify record where id=5; change zip to '01234'\n");
		modifyAction("5", "zip", "01234");

		System.out.println();
		
		System.out.println("\n        4.8 Modify record where id=5; change phone1 to '123-456-7890'\n");
		modifyAction("5", "phone1", "123-456-7890");

		System.out.println();
		
		System.out.println("\n        4.9 Modify record where id=5; change phone2 to '987-654-3210'\n");
		modifyAction("5", "phone2", "987-654-3210");

		System.out.println();
		
		System.out.println("\n        4.10 Modify record where id=5; change email to 'a@b.com'\n");
		modifyAction("5", "email", "a@b.com");

		System.out.println();
		
		System.out.println("\n        4.11 Modify record where id=5; change web to 'a.com'\n");
		modifyAction("5", "web", "a.com");
		
		System.out.println();
		
		System.out.println("\n        4.12 Modify record where id=5000; change web to 'a.com' (Record does not exist)\n");
		modifyAction("5000", "web", "a.com");
		
		System.out.println();
		
		System.out.println("\n        4.13 Modify record where id=5; change abc to 'a.com' (Field does not exist)\n");
		modifyAction("5", "abc", "a.com");
		
		

		
		System.out.println("\n\n\n\n\n");
		
		
		
		
		System.out.println("............................................................................................................");
		System.out.println("5) DELETE STATEMENTS");
		System.out.println("............................................................................................................");
		
		System.out.println("\n        5.1 Delete record where id=8\n");
		deleteAction("id","8",true);

		System.out.println();
		
		System.out.println("\n        5.2 Delete record where last_name=Marrier\n");
		deleteAction("last_name","Marrier",true);

		System.out.println();
		
		System.out.println("\n        5.3 Delete record where state=OH\n");
		deleteAction("state","OH",true);

		System.out.println();
		
		System.out.println("\n\n        .....Try deleting same records again.....");
		
		System.out.println("\n        5.4 Delete record where id=8\n");
		deleteAction("id","8",true);

		System.out.println();
		
		System.out.println("\n        5.5 Delete record where last_name=Marrier\n");
		deleteAction("last_name","Marrier",true);

		System.out.println();
		
		System.out.println("\n        5.6 Delete record where state=OH\n");
		deleteAction("state","OH",true);

		System.out.println();
		
		
		
		
		
	}
}
