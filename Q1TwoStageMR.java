import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
* authors: Alireza Farasat(50060827) and Rahul Gopalsamy(50163719)
* This is a two stage MR program for anomaly detection in the class scheduling
* 
*/
public class Q1TwoStageMR {

  public static class Mapper1
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String loc, sem, delim;
	loc = "";
	sem = "";
	delim = ",";
	double util;
    	String[] line=value.toString().split(delim); 
    	if(line[2].equals("Unknown")|| line[7].equals("")||!StringUtils.isNumeric(line[7]) ||!StringUtils.isNumeric(line[8]) || line[8].equals("NA") || line[7].equals("0") || line[8].equals("0")) return;
	sem = line[1];
	loc = line[2].split(" ")[0];
	util = (Double.parseDouble(line[7])/Double.parseDouble(line[8]));
    	word.set(loc+" "+sem);
    	context.write(word,new DoubleWritable(util));
    }
  }

  public static class Reducer1
       extends Reducer<Text,DoubleWritable,Text,Text> {
    private DoubleWritable result = new DoubleWritable();
  	private Text results = new Text();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	double max=0;
	double avg = 0;
	double sum = 0;
	double sumsqr = 0;
	double nexVal = 0;
	int count = 0;
    	for(DoubleWritable val:values){
		nexVal = val.get();
		sum += nexVal;
		sumsqr += nexVal*nexVal;
    		if(max < nexVal){
			max = nexVal;
		}
		count++;
    	}
	avg = sum/count;
	double std;
	std = Math.sqrt((sumsqr-2*avg*sum+count*avg*avg)/(double)count);
    	result.set(max);
	result.set(avg);
	results.set(String.valueOf(max)+"\t"+String.valueOf(avg)+"\t"+String.valueOf(std));
    	context.write(key,results);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, DoubleWritable>{
  	Text word=new Text();
	Text word2 = new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] line=value.toString().split("\\t");
	double util = Double.parseDouble(line[1]);
	double avg = Double.parseDouble(line[2]);
	double std = Double.parseDouble(line[3]);
	if(util < avg+3*std){
		return;
	} 
	String[] keys = line[0].split(" ");
	word.set(keys[0]+" "+keys[1]);
	word2.set(keys[1]+" "+keys[2]+" "+String.valueOf(util));
	context.write(word,new DoubleWritable(1.0));

}
}

public static class Reducer2
  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

public void reduce(Text key, Iterable<DoubleWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {

	double sum = 0.0;
	for (DoubleWritable val:values){
		sum += val.get();
	} 
	System.out.println(key.toString()+"   "+sum);
	context.write(key,new DoubleWritable(sum));
}
}
  public static void main(String[] args) throws Exception {
    String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Find the standard deviation of utilization rate for each building");
    job.setJarByClass(Q1TwoStageMR.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
	//Second Stage MR
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Detecting anomalies");
    job2.setJarByClass(Q1TwoStageMR.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job2, new Path(temp));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
