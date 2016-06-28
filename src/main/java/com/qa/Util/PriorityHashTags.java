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

import org.apache.log4j.Logger;

import com.qa.sparkStreaming.SelfLearningExecutor;

import twitter4j.Status;

public class PriorityHashTags implements Serializable{
	
	final static Logger logger = Logger.getLogger(PriorityHashTags.class);

	public static Boolean testForPriorityHashTags(Status s) throws SQLException {

		logger.warn("testForPriorityHashTags");
		System.out.println("testForPriorityHashTags");
		List<String> priorityHashTagList = new ArrayList<>();

		Connection con = null;
		ResultSet rs = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDatabase", "root", "1234");
			stmt = con.createStatement();
			rs = stmt.executeQuery("select * from testDatabase.hashTagPriority");
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

		Boolean containsHashtags = false;
		
		for(String statuscontent : statusContent) {
			System.out.println("here");
			if(statuscontent.startsWith("#")) {
				System.out.println("found hashtag "+statuscontent);
				for(String hashtag : priorityHashTagList) {
					if(statuscontent.equals(hashtag)) containsHashtags = true;
				}
			}
		}

		return containsHashtags;
	}

	public static void writeNewHashTags(List<String> Status) throws SQLException {
		

		logger.warn("writeNewHashTags");
		System.out.println("writeNewHashTags - sysout");

		for (String s : Status) {
			if (s.startsWith("#")) {
				Connection con = null;
				Statement stmt = null;

				try {
					Class.forName("com.mysql.jdbc.Driver");
					con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDatabase", "root", "1234");
					stmt = con.createStatement();
					stmt.executeUpdate("Insert into testDatabase.hashTagNew values (\'"+s+"\',1)");

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
