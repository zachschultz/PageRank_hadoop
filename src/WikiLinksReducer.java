import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {
    @Override
	public void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context output) throws IOException, InterruptedException {
        
    	System.out.println("FIRST REDUCER");
        String links = "";        
        boolean isNotRedLink = false;
        
        System.out.println("KEY");
        System.out.println(key);
        
        Iterator<Text> vIter = values.iterator();
                
        
        System.out.println("Inlinks");
        // Brett concern (and zach's): if n pages link to a redlink
        // we will iterate n times and it could be wasteful
        while(vIter.hasNext()){
        	String v = vIter.next().toString();
        	
        	// Check first outlink is not #, if so, it is a redlink
        	if (v.equals("!@#$")) {
        		isNotRedLink = true;
        		continue;
        		
       		} else {
       			links += v + " ";
       			continue;
       		}
        }
        
        // If the key is not a redlink, send it to the output
        if (isNotRedLink) {
        	
        	String[] linkSplit = links.split(" ");
        	
        	for (String link : linkSplit) {
            	try {
    				output.write(new Text(link), new Text(key));
    			} catch (InterruptedException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
        	}

        	System.out.println(links);
        	
        
        } else {
        	
        	System.out.println(key + " IS A REDLINK");
//        	try {
//				output.write(key, new Text("BLEG"));
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}

        }
     }
}