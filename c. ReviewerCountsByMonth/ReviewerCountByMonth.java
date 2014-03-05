package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.TextInputFormat;


  	public class ReviewerCountByMonth {

		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  	     private final static IntWritable one = new IntWritable(1);
		 String record = "";

  			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);

			   // This is going to build a string like this: reviewerId YYYY MM
			   if (line.contains("review/userId:"))
			   {
				   tokenizer.nextToken();
				   record = tokenizer.nextToken();
				}
				else if (line.contains("review/time:"))
				{
					tokenizer.nextToken();
					Calendar mydate = Calendar.getInstance();
					mydate.setTimeInMillis(Long.valueOf(tokenizer.nextToken())*1000);
		            String month = Integer.toString(mydate.get(Calendar.MONTH) + 1);
		            if (month.length() < 2)
		                month = "0" + month;
					record = record + " " + mydate.get(Calendar.YEAR) + " " + month;
					output.collect(new Text(record), one);
					record = "";
				}
		 	}
    	}

    	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

    	       	int sum = 0;
    	       	while (values.hasNext())
    	       	{
     	         	sum += values.next().get();
     	       	}
				output.collect(key,new IntWritable(sum));
    	     }
    	}


    	public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
  	      private final static IntWritable one = new IntWritable(1);

			public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			   // this is just going to reverse the order of the input
			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens())
			   {
				   String reviewer=tokenizer.nextToken();
				   String year=tokenizer.nextToken();
				   String month=tokenizer.nextToken();
				   int Count=Integer.parseInt(tokenizer.nextToken());

				   output.collect(new IntWritable(Count), new Text(reviewer + " " + year + " " + month));
			   }
			 }
		}

    	public static class SecondReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    	   public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

				// this will just output the map in key descending order
				while (values.hasNext())
			   	{
					Text id=values.next();
			   		output.collect(key, id);
				}
    	     }
    	   }

		public static class ThirdMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		  private final static IntWritable one = new IntWritable(1);
		  private int previous = 0;
		  private int count = 0;
		  private int max = 10;

  			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   // this will collect just the first 10 lines, but more if some of the keys are the same
			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens())
			   {
				   String num=tokenizer.nextToken();
				   String reviewer=tokenizer.nextToken();
				   String year = tokenizer.nextToken();
				   String month = tokenizer.nextToken();
				   if (count < max) {
					   if (previous != Integer.parseInt(num))
					   {
						   ++count;
						   previous = Integer.parseInt(num);
					   }
					output.collect(new Text(reviewer + " " + year + " " + month + " " + num), one);
					}
			   }
			}
		}

       	public static class ThirdReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
					// direct output of keys only
	 				output.collect(key, null);
    	     }
       }



       public static void main(String[] args) throws Exception {

		 JobConf conf = new JobConf(ReviewerCountByMonth.class);
		 conf.setJobName("ReviewerCountByMonth");
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(IntWritable.class);
		 conf.setMapperClass(Map.class);
		 conf.setReducerClass(Reduce.class);
		 // this will ensure that the movies file is not split because the first map needs to read from two lines.
		 conf.set("mapred.min.split.size","9332887470");
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path("./temp1"));
		 JobClient.runJob(conf);

		  JobConf conf1 = new JobConf(ReviewerCountByMonth.class);
		  conf1.setJobName("ReviewerCountByMonth2");
		  conf1.setOutputKeyClass(IntWritable.class);
		  conf1.setOutputValueClass(Text.class);
		  // this will reverse sort on the IntWritable key
		  conf1.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		  conf1.setMapperClass(SecondMap.class);
		  conf1.setReducerClass(SecondReduce.class);
		  conf1.setInputFormat(TextInputFormat.class);
		  conf1.setOutputFormat(TextOutputFormat.class);
		  FileInputFormat.setInputPaths(conf1, new Path("./temp1"));
		  FileOutputFormat.setOutputPath(conf1, new Path("./temp2"));
		  JobClient.runJob(conf1);

		  JobConf conf2 = new JobConf(ReviewerCountByMonth.class);
		  conf2.setJobName("ReviewerCountByMonth3");
		  conf2.setOutputKeyClass(Text.class);
		  conf2.setOutputValueClass(IntWritable.class);
		  conf2.setMapperClass(ThirdMap.class);
		  conf2.setReducerClass(ThirdReduce.class);
		  conf2.set("mapred.min.split.size","9332887470");
		  conf2.setInputFormat(TextInputFormat.class);
		  conf2.setOutputFormat(TextOutputFormat.class);
		  FileInputFormat.setInputPaths(conf2, new Path("./temp2"));
		  FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
		  JobClient.runJob(conf2);
	   }
	}
