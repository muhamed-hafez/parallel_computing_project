package codon.pair.la;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
		
		HashMap<String, Integer> codonHash = new HashMap<String, Integer>();

		int temp;
		
		for (String codon : codonList)
		{
			if(!codonHash.containsKey(codon))
				codonHash.put(codon, 1);
			else
			{
				temp = codonHash.get(codon);
				codonHash.remove(codon);
				codonHash.put(codon, temp+1);
			}
		}
		
		for (String codon : codonHash.keySet())
			context.write(new Text(codon), new IntWritable(codonHash.get(codon)));
	}
}
