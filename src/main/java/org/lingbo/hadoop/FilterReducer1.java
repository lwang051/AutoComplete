package org.lingbo.hadoop;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer1 extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
	
	private int topK;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		topK = conf.getInt("topK", 3);
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		Comparator<Following> comparator = new Comparator<Following>() {
			public int compare(Following o1, Following o2) {
				if(o1.count == o2.count) {
					return o1.str.compareTo(o2.str);
				}else {
					return o1.count - o2.count;
				}
			}
		};
		PriorityQueue<Following> pq = new PriorityQueue<Following>(1, comparator);
		for(Text value : values) {
			String[] array = value.toString().trim().split(":");
			Following f = new Following(array[0].trim(), Integer.valueOf(array[1].trim()));
			if(pq.size() < topK) {
				pq.add(f);
			}else {
				if(comparator.compare(f, pq.peek()) > 0) {
					pq.poll();
					pq.add(f);
				}
			}
		}
		while(!pq.isEmpty()) {
			Following f = pq.poll();
			context.write(
				new DBOutputWritable(key.toString().trim(), f.str, f.count), 
				NullWritable.get());
		}
	}
}
