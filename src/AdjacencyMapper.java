import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class AdjacencyMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException {    
    	
		System.out.println("MAPPER:");
		
		
		// split value on tab
//		left value is key
//		right value is the outlink
		
		String[] keyAndLink = value.toString().split("\t");
		System.out.println("CHECKING FOR BLANK INLINK THING");
		System.out.println(keyAndLink[0].length());
		System.out.println("checked it");
		
		try {
			// Our link has no outlinks, but is still real
			if (keyAndLink[0].length() == 0)
				output.write(new Text(keyAndLink[1]), new Text("no outlinks"));
			else
				output.write(new Text(keyAndLink[0]), new Text(keyAndLink[1]));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
