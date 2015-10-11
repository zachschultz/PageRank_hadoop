import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AdjacencyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
	public void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context output) throws IOException, InterruptedException {
  
    	System.out.println("REDUCER");
    	System.out.println(values);
    	
    	Iterator<Text> vIter = values.iterator(); 
    	String list = "";
    	while (vIter.hasNext()) {
    		String v = vIter.next().toString();
    		list += v + "   ";
    		
    	}
    	output.write(key, new Text(list));
    }
}