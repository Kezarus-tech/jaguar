package com.kezarus.jaguar;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import com.kezarus.jaguar.dao.ConnectionFactory;
import com.kezarus.jaguar.dao.QueryExecutor;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;


public class MainEngine {

	//DATABASE
	//CONNECTION STRING
	//FILE PATH
	String fileNameAndPath;
	String url = "jdbc:mysql://localhost:3306/test";
	String user = "root";
	String pass = "1234";
	String table = "TEST";
	
	public MainEngine( String fileNameAndPath ) {
		this.fileNameAndPath = fileNameAndPath;
	}
	
	public void run() {
		//CONNECTION POOL SETUP
		ConnectionFactory connFactory = new ConnectionFactory(url, user, pass);
		
		ArrayList<Object[]> arrValues = new ArrayList<Object[]>();
		CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
		
		
		try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(fileNameAndPath)).withCSVParser(csvParser).build()) {
			String[] values = null;
			
			//GET HEADER
			String[] header = csvReader.readNext();
			
			long index = 0;
			while (true) {
				index++;
				values = csvReader.readNext();

				if (values != null) {
					arrValues.add(values);
				}

				if (index % 100000 == 0 || (values == null && arrValues.size() > 0)) {
					//New Batch index
					QueryExecutor.insertBatch(connFactory.getConnection(), table, header, arrValues);
					arrValues.clear();
				}

				if (values == null) {
					break;
				}
			}
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	
	
}
