package org.myorg;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.TextInputFormat;


  	public class ProductCount {

		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  	     private final static IntWritable one = new IntWritable(1);

  	     	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   if (line.contains("product/productId:"))
			   {
				   // The first token is the string "product/productId:", so just discard it
				   tokenizer.nextToken();
				   // The second token is the actual product ID, so store it
				   output.collect(new Text(tokenizer.nextToken()), one);
				}
		 	}
    	}

    	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	       int sum = 0;
    	       while (values.hasNext()) {
     	         sum += values.next().get();
     	       }
    	       // Output as productID count, making count an IntWritable for the second map to sort
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
				   // productId comes first here
				   String product=tokenizer.nextToken();
				   int count=Integer.parseInt(tokenizer.nextToken());
                   // output just reverses order
				   output.collect(new IntWritable(count), new Text(product));
			   }
			 }
		}

    	public static class SecondReduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    	     public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

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

			   String line = value.toString();
			   StringTokenizer tokenizer = new StringTokenizer(line);
			   while (tokenizer.hasMoreTokens())
			   {
				   String num=tokenizer.nextToken();
				   String reviewer=tokenizer.nextToken();
				   if (count < max)
				   {
						// account for ties by only increment count if the current num is different from the previous
						if (previous != Integer.parseInt(num))
					    {
						   ++count;
						   previous = Integer.parseInt(num);
					    }
					output.collect(new Text(reviewer + " " + num), one);
					}
			   	}
			 }
		}

    	public static class ThirdReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
    	  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				 // simply output everything coming from the map
	   			 output.collect(key, null);
    	     }
    	   }

   	   public static void main(String[] args) throws Exception {

    	  JobConf conf = new JobConf(ProductCount.class);

    	  conf.setJobName("ProductCount1");
     	  conf.setOutputKeyClass(Text.class);
     	  conf.setOutputValueClass(IntWritable.class);
    	  conf.setMapperClass(Map.class);
    	  conf.setReducerClass(Reduce.class);
    	  conf.setInputFormat(TextInputFormat.class);
    	  conf.setOutputFormat(TextOutputFormat.class);
     	  FileInputFormat.setInputPaths(conf, new Path(args[0]));
     	  FileOutputFormat.setOutputPath(conf, new Path("./temp1"));
   	      JobClient.runJob(conf);


		  JobConf conf1 = new JobConf(ProductCount.class);
		  conf1.setJobName("ProductCount2");
		  conf1.setOutputKeyClass(IntWritable.class);
		  conf1.setOutputValueClass(Text.class);
		  // This will reverse-sort the keys when the keys are IntWritables
		  conf1.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
		  conf1.setMapperClass(SecondMap.class);
		  conf1.setReducerClass(SecondReduce.class);
		  conf1.setInputFormat(TextInputFormat.class);
		  conf1.setOutputFormat(TextOutputFormat.class);
		  FileInputFormat.setInputPaths(conf1, new Path("./temp1"));
		  FileOutputFormat.setOutputPath(conf1, new Path("./temp2"));
		  JobClient.runJob(conf1);


		  JobConf conf2 = new JobConf(ProductCount.class);
		  conf2.setJobName("ProductCount3");
		  conf2.setOutputKeyClass(Text.class);
		  conf2.setOutputValueClass(IntWritable.class);
		  conf2.setMapperClass(ThirdMap.class);
		  conf2.setReducerClass(ThirdReduce.class);
		  // This will make sure that the file being mapped is not split
		  conf2.set("mapred.min.split.size","9332887470");
		  conf2.setInputFormat(TextInputFormat.class);
		  conf2.setOutputFormat(TextOutputFormat.class);
		  FileInputFormat.setInputPaths(conf2, new Path("./temp2"));
		  FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
		  JobClient.runJob(conf2);
		}
   }
