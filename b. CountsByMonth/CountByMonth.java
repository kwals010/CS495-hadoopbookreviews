package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.TextInputFormat;


  	public class CountByMonth {

		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  	      private final static IntWritable one = new IntWritable(1);

  	      	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);

				// This is going to build a key that looks like this: YYYY MM
				if (line.contains("review/time:"))
				{
					tokenizer.nextToken();
					Calendar mydate = Calendar.getInstance();
					mydate.setTimeInMillis(Long.valueOf(tokenizer.nextToken())*1000);
		            String month = Integer.toString(mydate.get(Calendar.MONTH) + 1);
		            if (month.length() < 2)
		            {
		                month = "0" + month;
					}
					output.collect(new Text(mydate.get(Calendar.YEAR) + " " + month), one);
				}
		 	}
    	}

    	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    	   public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	       int sum = 0;
    	       while (values.hasNext()) {
     	         sum += values.next().get();
     	       }
			   // puts the key and sum into the key with a space
    	       output.collect(new Text(key + " " + sum), null);
    	     }
    	   }

	   public static void main(String[] args) throws Exception {

		 JobConf conf = new JobConf(CountByMonth.class);
		 conf.setJobName("CountByMonth");
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(IntWritable.class);
		 conf.setMapperClass(Map.class);
		 conf.setReducerClass(Reduce.class);
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    	 JobClient.runJob(conf);
	   }
	}