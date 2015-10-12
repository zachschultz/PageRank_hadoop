import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {
	 
    public static void main(String[] args) throws Exception {
        PageRank pageRanking = new PageRank();
        String job1Input = args[0];
        String job1Output = args[1]; 
        
        
        boolean job1Done = pageRanking.runXmlParsing(job1Input, job1Output);
        
        // Error in job 1
        if (job1Done != true)
        	System.exit(1);
        
        System.out.println(job1Output);
        String j1OPR = job1Output + "/part-r-00000";
        File f = new File(j1OPR);
        System.out.println(f.exists());
        
        String output = "";
        BufferedReader br = null;
        try {
        	br = new BufferedReader(new FileReader(f));
        	String text = "";
        	while ((text = br.readLine()) != null) {
        		output += text + "\n";
        	}
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
            }
        }
        
        System.out.println(output);
        
        boolean job2Done = pageRanking.getAdjacencyGraph(new Path(j1OPR), new Path("s3://lab03bucket/iter2"));
    }
 
    private boolean getAdjacencyGraph(Path inputPath, Path path) throws IOException {
        Configuration conf = new Configuration();
        
    	Job job2 = Job.getInstance(conf);
        job2.setJarByClass(PageRank.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // Our class to parse links from content.
        job2.setMapperClass(AdjacencyMapper.class);
        job2.setReducerClass(AdjacencyReducer.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
       
        // Remove output if already exists        
        FileInputFormat.setInputPaths(job2, inputPath);
        FileOutputFormat.setOutputPath(job2, path);  
                     
        try {
			job2.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}    
        return true;
	}

	public boolean runXmlParsing(String inputPath, String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        
    	Job job1 = Job.getInstance(conf);
        job1.setJarByClass(PageRank.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        // Our class to parse links from content.
        job1.setMapperClass(WikiPageXMLMapper.class);
        job1.setReducerClass(WikiLinksReducer.class);
        
        job1.setInputFormatClass(XmlInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
       
        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));  
                     
        try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}    
        
        return true;
    }
}