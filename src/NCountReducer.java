import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
	public void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context output) throws IOException, InterruptedException {
        

        Iterator<Text> vIter = values.iterator();
        System.out.println("REDUCER");
        int count = 0;
        // Brett concern (and zach's): if n pages link to a redlink
        // we will iterate n times and it could be wasteful
        while(vIter.hasNext()){
        	
        	count++;
        	vIter.next();
        }
        
        String countStr = Integer.toString(count);
        countStr = countStr.trim();
        
        System.out.println(count);
        output.write(new Text("N = "), new Text(countStr));	
        System.out.println(count);
        
     }
}