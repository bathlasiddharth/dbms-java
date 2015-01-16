# dbms-java
Database Design Project (Low Level Implementation of DBMS in JAVA)


MyDatabase.java contains all the functions required to implement CRUD operation in DBMS. 
Java Collections are used to store indexes. The objects are stored in *.ndx files. 


Problem Statement:
This project involves the creation of a Java program capable of acting a database that can locate records based upon indexes on two (or more) fields.

The data is a single table of people along with their personal data. The data is provided in a file named us-500.csv. Field attribute names are provided in the first line of the data file.

Your program should operate entirely from the command line (no GUI).

Your program should store its data in a single binary file and each index should be a separate (either binary or text) file.

Your program should have the following functions and properties:
- Your program should be named MyDatabase. The main method should therefore be contained in a file named MyDatabase.java.
- Your program should store its database in a file named "data.db"
- Index files should be named for the field(s) being indexed and have the extension .ndx, e.g. id.ndx, city.ndx.
- Your Program should read and write to data.db using the Java API RandomAccessFile class.
- You are not allowed to use the readLine() method – the start location of each record should be identified with a byte offset value.
- At least one of your record attributes must be a field of variable length (i.e. similar to SQL varchar). Thus, your records will have a variable length.
- You are free to use any field delimiter of your choice.
- Record locations in the data.db file should be identified by the the seek(long) method, where the long variable is the byte offset location from the beginning of the file.
- Index files may be either binary or text format:
- Your index files that index unique values (i.e. keys) should be <key, address> pairs where the key is the value being searched and the address is the byte offset location of the record in the data.db binary file.
- Your index files that index non-unique values should be <key, address_list> pairs where the key is the value being indexed and the address_list is a list of record addresses that contain the indexed value.
- You must have index files for the following three fields (additional fields are optional): id, last_name, state.

Requred Actions
- Select - given a field name on which there is an index (unique or non-unique), retrieve all records that match the index and display each record on a separate line.
- Insert - given a record in comma separated String format (with values single quoted) insert the record into data.db and create new entries in your index files.
   "'502','James','Butt','Benton, John B Jr','6649 N Blue Gum St','New Orleans','Orleans','LA','70116','504-621-8927','504-845-1427','jbutt@gmail.com','http://www.bentonjohnbjr.com'"
   Note: The record id field is the primary key. Like the EMPLOYEE ssn primary key, you do not need to automatically generate an id value – it should be manually provided by the DBA. Insert on duplicate record id should fail cleanly and return a useful message.
   Note: Records inserted into data.db may simply be appended to the end. Only indexes require re-sorting of entries.
- Delete - given a unique identifier (i.e. key) remove a record from data.db and remove its entry from all indexes.
   Note: Deleted records may be overwritten with zeroes in data.db, but do not require re-organizing the data file. Only indexes require re-ordering of entries.
   Delete on non-existant records should fail cleanly and return a useful message.
- Modify - given a unique identifier, field_name, and new value of an existing record, update the old value of field_name with new value.
   Modify of a non-existant record identifier should fail cleanly and return a useful message.
- Count - your program should be able to return the total number of records in your database.

Program Compilation and Execution
Your program should compile with standard syntax, e.g. (e.g. "C:\> javac MyDatabase.java").
When invoked from the command line (e.g. "C:\> java MyDatabase"), your program should execute at least one of each of the above Required Actions in the main method, and print a useful narrative to the screen describing each action.
Your main() method should include clear syntax so that the grader can follow your execution logic and modify any action, recompile, and re-execute.

Data file is attached. Note that "zip" field should be handled as a String because some values begin with a relevant zero character.
us-500.csv 

