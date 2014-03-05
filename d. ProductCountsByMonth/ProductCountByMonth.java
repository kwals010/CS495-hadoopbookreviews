package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.TextInputFormat;


  	public class ProductCountByMonth {

		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  	      private final static IntWritable one = new IntWritable(1);
		  String record = "";

  	     	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   if (line.contains("product/productId:"))
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
    	       	output.collect(key, new IntWritable(sum));
    	    }
    	}


    	public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
  	      private final static IntWritable one = new IntWritable(1);

			public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens())
			   {
				   String product=tokenizer.nextToken();
				   String year=tokenizer.nextToken();
				   String month=tokenizer.nextToken();
				   int Count=Integer.parseInt(tokenizer.nextToken());

				   output.collect(new IntWritable(Count), new Text(product + " " + year + " " + month));
			   }
			 }
		}

		public static class SecondReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    	     public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

				while (values.hasNext())
			   	{
					Text Id=values.next();
	   				output.collect(key, Id);
				}
    	     }
    	}

		public static class ThirdMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		  private final static IntWritable one = new IntWritable(1);
		  private int previous = 0;
		  private int count = 0;
		  private int max = 10;

			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens())
			   {
				   String num=tokenizer.nextToken();
				   String product=tokenizer.nextToken();
				   String year = tokenizer.nextToken();
				   String month = tokenizer.nextToken();
				   if (count < max) {
					   if (previous != Integer.parseInt(num))
					   {
						   ++count;
						   previous = Integer.parseInt(num);
					   }
					output.collect(new Text(product + " " + year + " " + month + " " + num), one);
					}


			   }
			 }
		}

   	   public static class ThirdReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

   				 output.collect(key, null);
    	     }
   	   }

   	   public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(ProductCountByMonth.class);
		conf.setJobName("ProductCountByMonth");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		// this will ensure that the movie file does not get split
		conf.set("mapred.min.split.size","9332887470");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("./temp1"));
		JobClient.runJob(conf);

		JobConf conf1 = new JobConf(ProductCountByMonth.class);
		conf1.setJobName("ProductCountByMonth2");
		conf1.setOutputKeyClass(IntWritable.class);
		conf1.setOutputValueClass(Text.class);
		// this will reverse sort on key IntWritable
		conf1.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		conf1.setMapperClass(SecondMap.class);
	    conf1.setReducerClass(SecondReduce.class);
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf1, new Path("./temp1"));
		FileOutputFormat.setOutputPath(conf1, new Path("./temp2"));
		JobClient.runJob(conf1);

		JobConf conf2 = new JobConf(ProductCountByMonth.class);
		conf2.setJobName("ProductCountByMonth3");
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
