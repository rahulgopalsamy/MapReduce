import java.io.IOException;
import java.util.*;
import java.lang.*;
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
* This is a two stage MR program for regression analysis
* 
*/
public class Q3TwoStageMR {
  
  public static int forcastYear = 2017;
  public static class Mapper1
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text outPut = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String loc, sem, delim;
	loc = "";
	sem = "";
	delim = ",";
	String enrol;
    	String[] line=value.toString().split(delim); 
    	if(line[2].equals("Unknown")|| line[7].equals("")||!StringUtils.isNumeric(line[7]) ||!StringUtils.isNumeric(line[8]) || line[8].equals("NA") || line[7].equals("0") || line[8].equals("0")) return;
	sem = line[1].split(" ")[0];
	int year = Integer.parseInt(line[1].split(" ")[1]);
	if(year < 1990) return;
	loc = line[2].split(" ")[0];
	//util = (Double.parseDouble(line[7])/Double.parseDouble(line[8]));
	enrol = line[7];
	String courseName = line[6];
    word.set(courseName+","+sem);
	//System.out.println(word.toString()+" "+util);
	outPut.set(String.valueOf(year)+" "+enrol);
    context.write(word,outPut);
    }
  }

  public static class Reducer1
       extends Reducer<Text,Text,Text,Text> {
    private DoubleWritable result = new DoubleWritable();
  	private Text results = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	double a0,a1;
	double avgYear, avgEnrol;
	double sumYear, sumEnrol, sumEnY;
	double sumYSqr = 0;
	int count = 0;
	avgYear = 0; avgEnrol = 0; sumYear = 0; sumEnrol = 0; sumEnY = 0;
	Iterator<Text> iterator=values.iterator();
	ArrayList<Integer> year = new ArrayList<Integer>();
	ArrayList<Integer> enrol = new ArrayList<Integer>();
    	while(iterator.hasNext()){
		String nexVal = iterator.next().toString();
		int y = Integer.parseInt(nexVal.split(" ")[0]);
		int e = Integer.parseInt(nexVal.split(" ")[1]);
		sumYear += y;
		sumEnrol += e;
		year.add(y);
		enrol.add(e);
		sumEnY += y*e;
		sumYSqr += y*y;  
		count ++;
    	}
	
	avgYear = sumYear/count;
	avgEnrol = sumEnrol/count;
	a1 = (sumEnY-avgEnrol*sumYear)/(sumYSqr-avgYear);
	a0 = avgEnrol-a1*avgYear;
	results.set(String.valueOf(a0)+"\t"+String.valueOf(a1));
	//System.out.println(key.toString()+"\t"+results.toString());
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
	double a0 = Double.parseDouble(line[1]);
	double a1 = Double.parseDouble(line[2]);
	String[] keys = line[0].split(",");
	word.set(keys[0]);
	values.set(String.valueOf(a0)+" "+String.valueOf(a1)+" "+keys[1]);
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
	ArrayList<String> forcast =new ArrayList<String>();
	while(iterator.hasNext()){
		String value1=iterator.next().toString();
		double a0 = Double.parseDouble(value1.split(" ")[0]);
		double a1  = Double.parseDouble(value1.split(" ")[1]);
		String sem = value1.split(" ")[2];
		Double forcastValue = a0+a1*forcastYear;
		if (forcastValue < 0 || forcastValue.isNaN()){
			forcast.add(sem+": "+"Not Available");
		}else{
			forcast.add(sem+": "+String.valueOf(forcastValue));
		}
	}
	outPut.set(forcast.toString());
	//System.out.println(key.toString()+" "+t);
	context.write(key,outPut);
	//if(!iterator.hasNext())return;
	//System.out.println("Reducer 2");
	//System.out.println(key.toString()+"   "+t);
}
}
  public static void main(String[] args) throws Exception {
    String temp="Temp";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Calculation Parameter of regression for each course");
    job.setJarByClass(Q3TwoStageMR.class);
    job.setMapperClass(Mapper1.class);
    //job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Predicting the number of students' enrolments in each semester for each course ");
    job2.setJarByClass(Q3TwoStageMR.class);
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
