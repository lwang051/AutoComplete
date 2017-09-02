package org.lingbo.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for(Text value : values) {
			sb.append(" ");
			sb.append(value.toString());
		}
		context.write(key, new Text(sb.toString().trim()));
	}
}
