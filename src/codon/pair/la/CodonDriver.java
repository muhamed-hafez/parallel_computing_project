package codon.pair.la;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; /*This class is responsible for running map reduce job*/

public class CodonDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: CodonDriver <input path> <outputpath>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(CodonDriver.class);
		job.setJobName("Pair Codon");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CodonMapper.class);
		job.setReducerClass(CodonReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		CodonDriver driver = new CodonDriver();
		long start = System.currentTimeMillis();
		int exitCode = ToolRunner.run(driver, args);
		System.out.println("ramzy test : " + (System.currentTimeMillis() - start));
		System.exit(exitCode);
	}
}
