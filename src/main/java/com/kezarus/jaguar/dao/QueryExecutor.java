package com.kezarus.jaguar.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class QueryExecutor {
	public static void insertBatch(Connection connection, String tableName, String[] fieldNames, ArrayList<Object[]> arrValues) throws SQLException {
		StringBuilder fields = new StringBuilder();
		StringBuilder valuesMarker = new StringBuilder();
		String sql;
		int arrIndex = 0;

		for (int i = 0; i < fieldNames.length; i++) {
			fields.append("`" + fieldNames[i] + "`,");
			valuesMarker.append("?,");
		}
		fields.deleteCharAt(fields.length() - 1);
		valuesMarker.deleteCharAt(valuesMarker.length() - 1);
		sql = "INSERT INTO TEST (" + fields.toString() + ") VALUES(" + valuesMarker.toString() + ");";

		connection.setAutoCommit(false);

		PreparedStatement ps = connection.prepareStatement(sql);

		for (Object[] values : arrValues) {

			// SET PARAMETERS
			for (int i = 0; i < values.length; i++) {
				ps.setObject(i + 1, values[i]);
			}

			arrIndex++;
			ps.addBatch();
			if (arrIndex % 10000 == 0 || arrIndex == arrValues.size()) {
				ps.executeBatch(); // Execute every 10000 items.
				System.out.print(".");
			}
		}

		connection.setAutoCommit(true);
		connection.close();
	}
}
