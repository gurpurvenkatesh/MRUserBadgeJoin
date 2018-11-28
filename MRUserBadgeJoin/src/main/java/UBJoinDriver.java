import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UBJoinDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {



		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();	/* This Generic Parsers class fetches all command line arguments */
		
		if(otherArgs.length != 6){		/* Checking 4 command line arguments. 2 input path, 1 output path, 1 start time, 1 end time, 1 Join type*/
			System.err.println("Command line parameters not entered correctly");
			System.exit(1);
		}
		
		String log1 = otherArgs[3];
		String log2 = otherArgs[4];
		String joinType = otherArgs[5];
		
		Job job = new Job(conf, "MapReduce Joins");	
		job.getConfiguration().set("join.type", joinType);		/* Setting join type configuration property */
		job.getConfiguration().set("log1", log1);
		job.getConfiguration().set("log2", log2);
		job.setJarByClass(UBJoinDriver.class);		
			
		/* Multiple inputs are passed into the mapper through the class MultipleInputs
		 * Parameters are 
		 * job object, 
		 * Path of input file, 
		 * Type of input file content & 
		 * the mapper associated with that input file */
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class); 
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BadgesMapper.class);
				
		job.setReducerClass(JoinReducer.class);	
		job.setNumReduceTasks(1);	
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Integer.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));	
			
		return (job.waitForCompletion(true) ? 0 : 1);		
	}
	
	public static void main(String args[]) throws Exception {

	    int res = ToolRunner.run(new UBJoinDriver(), args); 
	    System.exit(res);
	}

}
