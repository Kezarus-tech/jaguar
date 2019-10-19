package com.kezarus.jaguar.dao;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

//https://www.baeldung.com/java-connection-pooling
public class ConnectionFactory {
    private BasicDataSource ds = new BasicDataSource();
         
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
     
    public ConnectionFactory(String url, String user, String pass){
    	ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setMinIdle(5);
        ds.setMaxIdle(10);
        ds.setMaxOpenPreparedStatements(100);
    }
}

