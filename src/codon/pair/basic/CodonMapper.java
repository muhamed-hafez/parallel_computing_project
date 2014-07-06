package codon.pair.basic;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CodonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		List<String> codonList = new LinkedList<String>();
		Collections.addAll(codonList, line.split("(?<=\\G......)"));
		Collections.addAll(codonList, line.substring(3).split("(?<=\\G......)"));
		
		for (String codon : codonList)
			context.write(new Text(codon), new IntWritable(1));		
	}
}
