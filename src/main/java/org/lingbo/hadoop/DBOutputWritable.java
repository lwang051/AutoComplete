package org.lingbo.hadoop;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable {
	
	private String starting_phrase;
	private String following_phrase;
	private int count;
	
	public DBOutputWritable(String starting_phrase, String following_phrase, int count) {
		this.starting_phrase = starting_phrase;
		this.following_phrase = following_phrase;
		this.count = count;
	}
 
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1, starting_phrase);
		statement.setString(2, following_phrase);
		statement.setInt(3, count);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		this.starting_phrase = resultSet.getString(1);
		this.following_phrase = resultSet.getString(2);
		this.count = resultSet.getInt(3);
	}
	
}
