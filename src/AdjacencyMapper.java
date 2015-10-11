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
		
//		System.out.println(value);
		
		String[] keyAndLink = value.toString().split("\t");
//		System.out.println("CHECKING FOR BLANK INLINK THING");
////		System.out.println(keyAndLink[0].length());
////		System.out.println(keyAndLink[0]);
////		System.out.println("checked it");
		
		String A = keyAndLink[0];
		String B = keyAndLink[1];
		
		try {
			// Our link has no outlinks, but is still real
			if (A.equals("$#@!") || A.length() == 0) {
//				System.out.println(B + " goes to " + A);
				output.write(new Text(B), new Text(" "));
			} else {
//				System.out.println(A + " goes to " + B);
				output.write(new Text(A), new Text(B));
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
//		System.out.println("______________________");
	}
}
