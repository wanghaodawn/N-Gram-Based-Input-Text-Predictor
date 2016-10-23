/*
 * P4.1 Bach Processing of MapReduce
 * Task1 Building an NGram Model
 *
 * This file find the result from 1gram to 5gram from wikipedia documents 
 *
 * Hao Wang - haow2
 */

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
	public static class NGRAMMapper extends Mapper<Object, Text, Text, IntWritable> {

		private int gc_count = 0;
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	        // Run GC to prevent OUT OF MEMORY ERROR
			if (gc_count == 5000) {
				System.gc();
				gc_count = 0;
			} else {
				gc_count++;
			}

	      	// Parse the string
	        String s = value.toString().toLowerCase().trim();

	        // Remove references
	        s = remove_refs(s);
	        // System.out.println(s);

	        // Remove urls
	        s = remove_urls(s);

	        // Preserve ' for splitting
	        s = s.replaceAll("'", "haowangdawnwanghao");
	        s = s.replaceAll("[^a-z]+", " ").trim();

	        // Remove apostrophes
	        s = remove_apostrophes(s);
	        s = s.replaceAll("[ ]+", " ").trim();
	        String[] words = s.split(" ");

	        // Trim and remove nulls
	        ArrayList<String> words_list = new ArrayList<String>();
	        for (int i = 0; i < words.length; i++) {
	        	String temp = words[i].trim();
	        	if (temp.isEmpty()) {
	        		continue;
	        	}
	        	words_list.add(temp);
	        }

	        // Generate ngram
	        generateNgram(words_list.toArray(new String[0]), context);
	    }

	    /*
		 * Generate Ngram
		 */
	    private void generateNgram(String[] words, Context context) throws IOException, InterruptedException {
	    	// 1gram
			for (int i = 0; i < words.length; i++) {
				word.set(words[i]);
				context.write(word, one);
			}
			// 2gram
			for (int i = 0; i + 1< words.length; i++) {
				word.set(words[i] + " " + words[i+1]);
				context.write(word, one);
			}
			// 3gram
			for (int i = 0; i + 2 < words.length; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2]);
				context.write(word, one);
			}
			// 4gram
			for (int i = 0; i + 3 < words.length; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2] + " " + words[i+3]);
				context.write(word, one);
			}
			// 5gram
			for (int i = 0; i + 4 < words.length; i++) {
				word.set(words[i] + " " + words[i+1] + " " + words[i+2] + " " + words[i+3] + " " + words[i+4]);
				context.write(word, one);
			}
	    }

	    /*
		 * Remove the <ref > and </ref> 
		 */
		private String remove_refs(String s) {
			s = s.replaceAll("<ref[^>]*>", " ");
    		s = s.replaceAll("</ref>", " ");
			return s;
		}

		/*
		 * Remove the urls in corpus
		 */
		private String remove_urls(String s) {
			s = s.replaceAll("(https?|ftp):\\/\\/[^ ]*", " ");
			// s = s.replaceAll("https?://\\S+\\s?", "");
			// s = s.replaceAll("http?://\\S+\\s?", "");
			// s = s.replaceAll("ftp?://\\S+\\s?", "");
			return s;
		}

		/*
		 * Remove apostrophes at the edge of each word while keep those in words
		 */
		private String remove_apostrophes(String s) {
			s = s.replaceAll("haowangdawnwanghao", "'");
			s = s.replaceAll("'(?![a-z])", " ");
    		s = s.replaceAll("(?<![a-z])'", " ");
	        return s;
		}
	}

	/*
	 * The reducer for ngram
	 */
	public static class NGRAMReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		private int gc_count = 0;

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// Run GC to prevent OUT OF MEMORY ERROR
			if (gc_count == 5000) {
				System.gc();
				gc_count = 0;
			} else {
				gc_count++;
			}

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			// Only same those ngrams that appreas more than 10 times
			if (sum <= 2) {
				return;
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/*
	 * The Main function
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "ngram");
	    job.setJarByClass(ngram.class);
	    job.setMapperClass(NGRAMMapper.class);
	    // job.setCombinerClass(NGRAMReducer.class);
	    job.setReducerClass(NGRAMReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}