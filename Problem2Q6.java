import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
* authors: Alireza Farasat(50060827) and Rahul Gopalsamy(50163719)
*The program answers questions related to the courses. 
*
*/
public class Problem2Q6 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private static IntWritable count;
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    String docu = value.toString();
    Scanner lines = new Scanner(docu);
	String line="";
	String[] tokens = new String[11];
	int counter0 = 0;
	String course;
	course = "";
	Text mapkey;
	String tempKey;
	int cap;
	cap = 0;
	String delim = ",";
    while(lines.hasNextLine()){
		line = lines.nextLine();
		if(line.split(delim).length==11){
			for (int i = 0; i <11;i++){
				tokens[i] = line.split(delim)[i];
			}
				course = tokens[8];
				cap = Integer.parseInt(tokens[9]);
				word.set(course);
				count = new IntWritable(cap);
				context.write(word, count);
			
		}
    }
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int max = 0;
      int min = 1000;
      int count = 0;
      int sum = 0;
      double avg = 0;
      for (IntWritable val : values) {
        if(max < val.get()){
                          max = val.get();
                           }
        if(min > val.get()){
                          min = val.get();
                           }
       sum += val.get();
      count ++;
      }
      avg = (double)sum / (double)count ; 
       result.set(String.valueOf(count)+"\t"+String.valueOf(sum)+"\t"+String.valueOf(max)+"\t"+String.valueOf(min)+"\t"+String.valueOf(avg));
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "testP1");
    job.setJarByClass(Problem2Q6.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

