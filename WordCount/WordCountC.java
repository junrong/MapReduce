/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountC {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		// private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//Initialize the HashMap
			HashMap<Text, IntWritable> RealWordMap = new HashMap<Text, IntWritable>();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String cur = itr.nextToken();
				// Only count the real words starting with letters "m n o p q"
				// (no matter capitalized)
				if ((cur.charAt(0) >= 'm' && cur.charAt(0) <= 'q')
						|| (cur.charAt(0) >= 'M' && cur.charAt(0) <= 'Q')) {
					Text word = new Text(cur);
					
					if (!RealWordMap.containsKey(word)) {
						// If the word never counted in HashMap, put it into HashMap
						RealWordMap.put(word, one);
					} else {
						// If the word already existing in HashMAP, change the value to plus one.
						IntWritable num = new IntWritable(RealWordMap.get(word).get() + 1);
						RealWordMap.put(word, num);
					}
				}
			}
			for (Text w : RealWordMap.keySet()) {
				// System.out.println(RealWordMap.get(w));
				context.write(w, RealWordMap.get(w));
			}

		}
	}

	/**
	 * 
	 * @author junrongyan Partitioner class
	 *
	 */
	public static class Letterpartitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String w = key.toString();
			Character w1 = Character.toLowerCase(w.charAt(0));
			//this is done to avoid performing mod with 0
			if(numReduceTasks == 0){
				return 0;
			}
			// Words starting with "M" or "m" assigned to reduce task 0
			if (w1 == 'm') {
				return 0;
			}
			// Words starting with "N" or "n" assigned to reduce task 1
			if (w1 == 'n') {
				return 1 % numReduceTasks;
			}
			// Words starting with "O" or "o" assigned to reduce task 2
			if (w1 == 'o') {
				return 2 % numReduceTasks;
			}
			// Words starting with "P" or "p" assigned to reduce task 3
			// Else, words starting with "Q" or "q" assigned to reduce task 4
			if (w1 == 'p') {
				return 3 % numReduceTasks;
			} else {
				return 4 % numReduceTasks;
			}
		}

	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountC.class);
		job.setMapperClass(TokenizerMapper.class);
		// Disable the combiner
		// job.setCombinerClass(IntSumReducer.class);
		// Setup the Partitioner
		job.setPartitionerClass(Letterpartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
