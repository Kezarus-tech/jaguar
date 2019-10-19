package com.kezarus.jaguar;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JaguarApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(JaguarApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String fileNameAndPath = "C:\\WORK\\SOURCES\\JDBC\\_ARCHIVES\\DUMP.TXT";
		
		MainEngine engine = new MainEngine(fileNameAndPath);
		
		engine.run();
		System.out.println("End of application");
	}

}
