import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;


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

        
        String AWS2 = "s3://lab03bucket/iter2";
        String local = "./testing";
        
        boolean job2Done = pageRanking.getAdjacencyGraph(new Path(j1OPR), new Path(AWS2));
        
        String j2OPR = AWS2 + "/part-r-00000";
        String AWSN = "s3://lab03bucket/N";
        String localN = "./N";
        

        boolean job3Done = pageRanking.getNCount(new Path(j2OPR), new Path(AWSN));
        
       
    }	
    
    
 
    private boolean getNCount(Path inputPath, Path path) throws IOException {
        Configuration conf = new Configuration();
        
    	Job job3 = Job.getInstance(conf);
        job3.setJarByClass(PageRank.class);
        
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        
        // Our class to parse links from content.
        job3.setMapperClass(NCountMapper.class);
        job3.setReducerClass(NCountReducer.class);
        
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
       
        // Remove output if already exists        
        FileInputFormat.setInputPaths(job3, inputPath);
        FileOutputFormat.setOutputPath(job3, path);  
        job3.setNumReduceTasks(1);
        try {
			job3.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}    
        
        return true;
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
        job2.setNumReduceTasks(1);
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
        job1.setNumReduceTasks(1);
                     
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