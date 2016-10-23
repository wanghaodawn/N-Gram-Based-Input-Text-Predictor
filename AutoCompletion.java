/*
 * P4.1 Bach Processing of MapReduce 
 * Bonus: Building a Language Model for Word Auto-completion
 *
 * This file rank words according to their conditional probability
 *
 * Hao Wang - haow2
 */

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AutoCompletion {
    public static class AutoCompletionMapper extends Mapper<Object, Text, Text, Text>{
        
        private int gc_count = 0;
        private Text phrase = new Text();
        private Text val = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Run GC to prevent OUT OF MEMORY ERROR
            if (gc_count == 5000) {
                System.gc();
                gc_count = 0;
            } else {
                gc_count++;
            }

            String line = value.toString();
            String[] lines = line.split("\t");

            // Illegal input
            if (lines.length != 2) {
                return;
            }

            // Get all words from the file
            String[] words = lines[0].trim().split(" ");

            // If 1gram, then it cannot be predicted, so jump it
            if (words.length != 1) {
                return;
            }

            // Remove null strings after trim
            String word = words[0].trim();
            if (word.length() == 0) {
                return;
            }

            // Get the number of the frequency of the series of words
            String num = lines[1].trim();

            // Generate the key-value pairs
            for (int i = 1; i < word.length(); i++) {
                // The key is the words except the last one
                String key_s = word.substring(0, i);

                // The value is the last word and the num of occurance
                String value_s = word + ":" + num;

                phrase.set(key_s);
                val.set(value_s);
                context.write(phrase, val);
            }

        }
    }

    public static class AutoCompletionReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put> {

        private int gc_count = 0;
        private byte[] column_family = Bytes.toBytes("word_rank");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Run GC to prevent OUT OF MEMORY ERROR
            if (gc_count == 5000) {
                System.gc();
                gc_count = 0;
            } else {
                gc_count++;
            }

            // get configuration
            int N = Integer.parseInt(context.getConfiguration().get("n"));

            // Use PriorityQueue to store and sort
            PriorityQueue<String> queue = new PriorityQueue<String>(11, new Comparator<String>() {
                public int compare(String s1, String s2){
                    String[] ss1 = s1.split(":");
                    String[] ss2 = s2.split(":");

                    // Handle Illegal input
                    if (ss1.length != 2 || ss2.length != 2) {
                        return 0;
                    }

                    // Get the count
                    int num1 = Integer.parseInt(ss1[1]);
                    int num2 = Integer.parseInt(ss2[1]);

                    // If count is equal, then look at the comparator of strings, let smaller in front
                    if (num1 == num2) {
                        return ss1[0].compareTo(ss2[0]);
                    }

                    // Compare two numbers
                    return num2 - num1;
                }
            });
            // Queue<String> queue = new LinkedList<String>();

            // Add all elementes in the list then PriorityQueue will sort them
            for (Text value: values) {
                queue.add(value.toString());
            }

            // Get the minimum of the size of queue and N
            int n = Math.min(queue.size(), N);

            // Save the result in the database
            String key_s = key.toString().trim();
            ImmutableBytesWritable hbaseKey = new ImmutableBytesWritable(Bytes.toBytes(key_s));
            Put put = new Put(Bytes.toBytes(key_s));

            for (int i = 0; i < n; i++) {
                String[] ss = queue.poll().trim().split(":");
                put.add(column_family, Bytes.toBytes(ss[0]), Bytes.toBytes(ss[1]));
            }
            context.write(hbaseKey, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("n", args[0]);

        Job job = Job.getInstance(conf, "hbase");
        // Set table name
        String tableName = "task4";

        // Set Mapper and Reducer
        job.setJarByClass(AutoCompletion.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(AutoCompletionMapper.class);
        job.setReducerClass(AutoCompletionReducer.class);

        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        TableMapReduceUtil.addDependencyJars(job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
