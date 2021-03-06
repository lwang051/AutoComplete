package org.lingbo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AutoComplete {
	// command: input output1 output2 nGram threshold
    public static void main( String[] args ) throws Exception {
        if(args.length < 5) {
        	throw new Exception("usage: input output nGram threshold topK");
        }
    	
    	Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", ".");
        conf1.set("nGram", args[2]);
        
        Job job1 = Job.getInstance(conf1);
        job1.setMapperClass(NGramMapper.class);
        job1.setReducerClass(NGramReducer.class);
        job1.setJarByClass(AutoComplete.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));
        
//-------------------------------------------------------------------------
        
        Configuration conf2 = new Configuration();
        conf2.set("threshold", args[3]);
        conf2.set("topK", args[4]);
        
        DBConfiguration.configureDB(conf2, 
        	"com.mysql.jdbc.Driver", 
        	"jdbc:mysql://192.168.0.3:8889/test",  // ip_address $ port
        	"root", 
        	"root");
        
        Job job2 = Job.getInstance(conf2);
        job2.setMapperClass(FilterMapper.class);
        job2.setReducerClass(FilterReducer1.class);
        job2.setJarByClass(AutoComplete.class);
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));   // path_to_our_connector
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path(args[1]));
        DBOutputFormat.setOutput(job2, "output", new String[] {"starting_phrase", "following_word", "count"});
        
        if(job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
        	System.exit(0);
        }
    }
    
}
