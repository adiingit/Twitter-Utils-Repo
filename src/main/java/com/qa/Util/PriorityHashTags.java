package com.qa.Util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import twitter4j.Status;

public class PriorityHashTags implements Serializable{

	public static Boolean testForPriorityHashTags(Status s) throws SQLException {

		List<String> priorityHashTagList = new ArrayList<>();

		Connection con = null;
		ResultSet rs = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDatabase", "root", "1234");
			stmt = con.createStatement();
			rs = stmt.executeQuery("select * from hashTagPriority");
			while (rs.next()) {
				priorityHashTagList.add(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			con.close();
		}

		List<String> statusContent = Arrays.asList(s.getText().split(" "));

		Boolean commanElements = priorityHashTagList.retainAll(statusContent);

		return commanElements;
	}

	public static void writePriorityHashTags(List<String> Status) throws SQLException {

		for (String s : Status) {
			if (s.startsWith("#")) {
				Connection con = null;
				Statement stmt = null;

				try {
					Class.forName("com.mysql.jdbc.Driver");
					con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDatabase", "root", "1234");
					stmt = con.createStatement();
					stmt.executeUpdate("Insert into testDatabase.hashTagPriority values (\'"+s+"\',1)");

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					stmt.close();
					con.close();
				}
			}
		}
	}
}
