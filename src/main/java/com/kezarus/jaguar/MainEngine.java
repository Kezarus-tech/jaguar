package com.kezarus.jaguar;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;

import org.mozilla.universalchardet.UniversalDetector;

import com.kezarus.jaguar.dao.ConnectionFactory;
import com.kezarus.jaguar.dao.QueryExecutor;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class MainEngine {
	private String fileNameAndPath;
	private char separator;
	private String url = "jdbc:mysql://localhost:3306/test";
	private String user = "root";
	private String pass = "1234";
	private String table = "TEST";

	public MainEngine(String fileNameAndPath, char separator) {
		this.fileNameAndPath = fileNameAndPath;
		this.separator = separator;
	}

	public void run() throws IOException {
		// CONNECTION POOL SETUP
		ConnectionFactory connFactory = new ConnectionFactory(url, user, pass);

		ArrayList<Object[]> arrValues = new ArrayList<Object[]>();
		CSVParser csvParser = new CSVParserBuilder().withSeparator(separator).build();

		// FILE DETECT FORMAT
		File file = new File(fileNameAndPath);
		String encoding = getFileCharset(file);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding));

		try (CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(csvParser).build()) {
			String[] values = null;

			// GET HEADER
			String[] header = csvReader.readNext();

			long index = 0;
			while (true) {
				index++;
				values = csvReader.readNext();

				if (values != null) {
					arrValues.add(values);
				}

				if (index % 100000 == 0 || (values == null && arrValues.size() > 0)) {
					// New Batch index
					QueryExecutor.insertBatch(connFactory.getConnection(), table, header, arrValues);
					arrValues.clear();
				}

				if (values == null) {
					break;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static String getFileCharset(File file) throws IOException {
		byte[] buf = new byte[4096];
		BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
		final UniversalDetector universalDetector = new UniversalDetector(null);
		int numberOfBytesRead;
		
		while ((numberOfBytesRead = bufferedInputStream.read(buf)) > 0 && !universalDetector.isDone()) {
			universalDetector.handleData(buf, 0, numberOfBytesRead);
		}
		
		universalDetector.dataEnd();
		String encoding = universalDetector.getDetectedCharset();
		if(encoding == null) {
			encoding = StandardCharsets.UTF_8.name();
		}
		
		universalDetector.reset();
		bufferedInputStream.close();
		return encoding;
	}

}
