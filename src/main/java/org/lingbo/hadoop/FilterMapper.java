package org.lingbo.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {
	
	private int threshold;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		threshold = conf.getInt("threshold", 2);
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] array = value.toString().trim().split("\\t");
		int count = Integer.valueOf(array[1].trim());
		if(count < threshold) {
			return;
		}
		String[] array1 = array[0].trim().split("\\s+");
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < array1.length - 1; i++) {
			sb.append(" ");
			sb.append(array1[i]);
		}
		context.write(new Text(sb.toString().trim()), new Text(array1[array1.length - 1] + ":" + count));
	}
}
