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
* This is a two stage MR program for statistically analysing if there is
* any difference between Spring and Fall courses
* 
*/
public class Q2TwoStageMR {

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
	sem = line[1].split(" ")[0];
	int year = Integer.parseInt(line[1].split(" ")[1]);
	if(!sem.equals("Fall") && !sem.equals("Spring") || year < 1990) return;
	loc = line[2].split(" ")[0];
	//util = (Double.parseDouble(line[7])/Double.parseDouble(line[8]));
	util = Double.parseDouble(line[7]);
	String courseName = line[6];
    	word.set(courseName+","+sem);
	//System.out.println(word.toString()+" "+util);
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
	double var;
	var = (sumsqr-2*avg*sum+count*avg*avg)/(double)count;
	results.set(String.valueOf(count)+"\t"+String.valueOf(avg)+"\t"+String.valueOf(var));
	System.out.println(key.toString()+"\t"+results.toString());
    	context.write(key,results);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, Text>{
  	Text word=new Text();
	Text values = new Text();

public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] line=value.toString().split("\\t");
	int count = Integer.parseInt(line[1]);
	double avg = Double.parseDouble(line[2]);
	double var = Double.parseDouble(line[3]);
	String[] keys = line[0].split(",");
	word.set(keys[0]);
	values.set(String.valueOf(avg)+" "+String.valueOf(var)+" "+String.valueOf(count)+" "+keys[1]);
	//System.out.println(word.toString()+" "+values.toString());
	context.write(word,values);

}
}

public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {
	private Text outPut = new Text();
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	Iterator<Text> iterator=values.iterator();
	String value1=iterator.next().toString();
	double avg1 = Double.parseDouble(value1.split(" ")[0]);
	double var1  = Double.parseDouble(value1.split(" ")[1]);
	int count1 = Integer.parseInt(value1.split(" ")[2]);
	String sem1 = value1.split(" ")[3];
	if(!iterator.hasNext())return;
	String value2=iterator.next().toString();
        double avg2 = Double.parseDouble(value2.split(" ")[0]);
        double var2  = Double.parseDouble(value2.split(" ")[1]);
        int count2 = Integer.parseInt(value2.split(" ")[2]);
	double t = (avg1-avg2)/(Math.sqrt((var1/count1)+(var2/count2)));
	String sem2 = value2.split(" ")[3];
	//System.out.println(key.toString()+" "+t);
	if (t > 1.96){
		outPut.set(sem1+" > "+sem2);
	}else if (t < -1.96){
		outPut.set(sem2+" > "+sem1);
	}else{
		outPut.set(sem1+" = "+sem2);
	}
	context.write(key,outPut);
	//if(!iterator.hasNext())return;
	//System.out.println("Reducer 2");
	//System.out.println(key.toString()+"   "+t);
}
}
  public static void main(String[] args) throws Exception {
    String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "calculating the mean and variance for each course in each semester");
    job.setJarByClass(Q2TwoStageMR.class);
    job.setMapperClass(Mapper1.class);
    //job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Statistically Comparing Spring and Fall enrolments");
    job2.setJarByClass(Q2TwoStageMR.class);
    job2.setMapperClass(Mapper2.class);
    //job2.setCombinerClass(FindIncreaseReducer.class);
    job2.setReducerClass(Reducer2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(temp));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
