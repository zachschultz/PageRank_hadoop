import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class NCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text("N = ");
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException {    
    			
		try {
			output.write(word, value);
			System.out.println(word);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
//		System.out.println("______________________");
	}
}
