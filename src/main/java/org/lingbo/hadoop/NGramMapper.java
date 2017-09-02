package org.lingbo.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private int n;
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		n = conf.getInt("nGram", 5);
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String sentence = value.toString();
		sentence = sentence.trim().toLowerCase();
		sentence = sentence.replaceAll("[^a-z]", " ");
		String[] words = sentence.trim().split("\\s+");  // must trim before split !!
		if(words.length < 2) {
			return;
		}
		for(int i = 0; i < words.length; i++) {
			StringBuilder sb = new StringBuilder();
			for(int j = 0; j < n && i + j < words.length; j++) {
				sb.append(" ");
				sb.append(words[i + j]);
				if(j > 0) {
					context.write(new Text(sb.toString().trim()), one);
				}
			}
		}
	}
}
