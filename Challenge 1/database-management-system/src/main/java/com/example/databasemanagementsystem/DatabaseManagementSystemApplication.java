package com.example.databasemanagementsystem;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.util.*;

@SpringBootApplication
public class DatabaseManagementSystemApplication {

	private Map<String, List<Map<String, Object>>> database;

	public DatabaseManagementSystemApplication(){
		this.database = new HashMap<>();
	}

	//Create a new database
	public void createDatabase(String databaseName){
		database.put(databaseName, new ArrayList<>());
	}

	//Add data into database
	public void insertDataIntoDatabase(String databaseName, Map<String, Object> data){
		List<Map<String,Object>> table = database.getOrDefault(databaseName, new ArrayList<>());
		table.add(data);
		saveDataIntoFile(databaseName,table);
	}

	//Save data into file
	private void saveDataIntoFile(String databaseName, List<Map<String, Object>> table) {
		try(Writer writer = new FileWriter(databaseName + ".json")){
			Gson gson = new Gson();
			gson.toJson(table, writer);
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	//Read data from file
	private List<Map<String,Object>> readDataFromFile(String databaseName) {
		List<Map<String,Object>> table = new ArrayList<>();
		try(Reader reader = new FileReader(databaseName+".json")){
			Gson gson = new Gson();
			table = gson.fromJson(reader, new TypeToken<List<Map<String,Object>>>(){}.getType());

		}
		catch (IOException e){
			e.printStackTrace();
		}
		return table;
	}

//	execute query
	public List<Map<String, Object>> executeQuery(String databaseName, String condition){
		List<Map<String, Object>> table = readDataFromFile(databaseName);
		List<Map<String, Object>> result = new ArrayList<>();

		if(condition != null && !condition.isEmpty()){
			if(condition.startsWith("SELECT * FROM")){
				String[] tokens = condition.split("\\s+");
				if(tokens.length > 0 && tokens[4].equals("WHERE")){
					String field = tokens[5];
					String value = tokens[7];

					for(Map<String, Object> row: table){
						if(row.containsKey(field) && row.get(field).toString().equals(value)){
							result.add(row);
						}
					}
				}
				else{
					System.out.println("Query is not valid");
				}
			}
			else{
				System.out.println("Query is not valid");
			}
		}
		else{
			System.out.println("2");
			result.addAll(table);
		}
		return result;
	}

	public static void main(String[] args) {
		DatabaseManagementSystemApplication dbms = new DatabaseManagementSystemApplication();

//		Create database
		dbms.createDatabase("example_db");

//		Add data into database
		Map<String, Object> data = new HashMap<>();
		data.put("id", "1");
		data.put("name", "Trinh Hoai Thanh");
		dbms.insertDataIntoDatabase("example_db", data);

//		Execute query
		List<Map<String, Object>> result = dbms.executeQuery("example_db", "SELECT * FROM example_db WHERE id = 1");

		System.out.println(result);
	}

}
