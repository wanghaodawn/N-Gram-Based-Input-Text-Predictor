package test;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ngram {

	/*
	 * The mapper for ngram
	 */
	public static class ngramMapper extends Mapper<Object, Text, Text, IntWritable> {

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {

	      	// Parse the string
	        String s = value.toString().toLowerCase().trim();

	        // Remove references
	        s = remove_refs(s);
	        // System.out.println(s);

	        // Remove urls
	        s = remove_urls(s);

	        // Preserve ' for splitting
	        s = s.replaceAll("'", "haowangdawnwanghao");
	        s = s.replaceAll("[^A-Za-z]+", " ").trim();

	        // Remove apostrophes
	        s = remove_apostrophes(s);

	        String[] words = s.split(" ");

			// 1-gram
			for (int i = 0; i < words.length; i++) {
				word.set(words[i]);
				context.write(word, one);
			}

			// 2-gram
			for (int i = 0; i < words.length - 1; i++) {
				word.set(words[i] + " " + words[i+1]);
				context.write(word, one);
			}

			// 3-gram
			for (int i = 0; i < words.length - 2; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2]);
				context.write(word, one);
			}

			// 4-gram
			for (int i = 0; i < words.length - 3; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2] + " " + words[i+3]);
				context.write(word, one);
			}

			// 5-gram
			for (int i = 0; i < words.length - 4; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2] + " " + words[i+3] + " " + words[i+4]);
				context.write(word, one);
			}
	    }

	    /*
		 * Remove the <ref > and </ref> 
		 */
		private String remove_refs(String s) {
			while (s.contains("<ref") || s.contains("</ref>")) {
				int i = s.indexOf("<ref");
				int j = s.indexOf(">", i);
				if (i != -1 && j > i) {
					s = s.substring(0, i) + " " + s.substring(j + 2);
				}

				i = s.indexOf("</ref>");
				if (i != -1) {
					s = s.substring(0, i) + " " + s.substring(i + 6);
				}
				// System.out.println(s);
			}
			return s;
		}

		/*
		 * Remove the urls in corpus
		 */
		private String remove_urls(String s) {
			s = s.replaceAll("https?://\\S+\\s?", "");
			s = s.replaceAll("http?://\\S+\\s?", "");
			s = s.replaceAll("ftp?://\\S+\\s?", "");
			return s;
		}

		/*
		 * Remove apostrophes at the edge of each word while keep those in words
		 */
		private String remove_apostrophes(String s) {
			String[] words = s.split(" ");

	        String temp = "";

	       	// replace ' in students' but keep ' in it's
	        for (int i = 0; i < words.length; i++) {
	        	String word = words[i].trim();
	        	word = word.replaceAll("haowangdawnwanghao", "'");
	        	
	        	int j = 0;
	        	while (j < word.length() && word.charAt(j) == '\'') {
	        		word = word.substring(j + 1);
	        		j++;
	        	}

	        	j = word.length() - 1;
	        	while (j >= 0 && word.charAt(j) == '\'') {
	        		word = word.substring(0, j);
	        		j--;
	        	}
	        	if (i < words.length-1) {
	        		temp += word + " ";
	        	} else {
	        		temp += word;
	        	}
	        }
	        return temp;
		}
	}

	/*
	 * The reducer for ngram
	 */
	public static class ngramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 2) {
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	/*
	 * The Main function
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ngram");
	    job.setJarByClass(ngram.class);
	    job.setMapperClass(ngramMapper.class);
	    job.setReducerClass(ngramReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
