import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class WikiPageXMLMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException {    
    	
		ArrayList<String> titleAndLinks = parseTitleAndText(value.toString());
		
		System.out.println(value);
		
		String t = titleAndLinks.remove(0);
		
		String pages = "!@#$ ";
		for (String link : titleAndLinks) {
			String otherPage = link;
			otherPage = checkForSubpageLinks(otherPage);
			otherPage = otherPage.replace(" ", "_");
        	otherPage = otherPage.split("\\|")[0];
        	otherPage = checkForDuplicates(otherPage, pages);
        	
        	otherPage = checkForInterWiki(otherPage);
        	otherPage = checkForFileLink(otherPage);
        	
        	otherPage = (otherPage.indexOf(":") == -1) ? otherPage : "";
        	otherPage = (otherPage.indexOf("#") == -1) ? otherPage : "";
        	
        	
        	int linkLength = otherPage.length();
        	if (linkLength >= 3) {
        		if (otherPage.lastIndexOf(" ") == linkLength - 1 && 
        				otherPage.substring(0, linkLength-1).lastIndexOf(" ") == linkLength - 2) {
        			System.out.println("TWO SPACES");
        			otherPage = "";

        		}
        	}
        	
        	if (otherPage == "")
        		continue;
       
        	Text oP = new Text(otherPage);
        	
        	pages += oP + " ";
        	
        	try {
				output.write(new Text(oP), new Text(t));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("KEY");
		System.out.println(t);
		System.out.println("links");
		System.out.println(pages);
		
		
        boolean noOutlinks = pages.equals("!@#$ ");
        // Designate this page as not a redlink
        try {
        	// !@#$ is the key for pages that are titles
			output.write(new Text(t), new Text("!@#$"));
			
			// This will ensure that we dont lose the node when we return from ingraph
			if (noOutlinks)
				output.write(new Text(t), new Text("$#@!"));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	} 

	private String checkForFileLink(String otherPage) {
		if (otherPage.length() < 5)
			return otherPage;
		if (otherPage.substring(0, 5).equals("File:"))
			return "";
		
		return otherPage;
	}

	private String checkForInterWiki(String otherPage) {

		if (otherPage.length() < 2)
			return otherPage;
		
		if (otherPage.indexOf(":") == 0 && otherPage.indexOf(":", 1) != -1)
			return "";
		if (otherPage.substring(0,2).equals("m:"))
			return "";
		
		return otherPage;
	}

	private String checkForSubpageLinks(String otherPage) {
		if (otherPage.length() < 3)
			return otherPage;
		if (otherPage.substring(0,3).equals("../") || otherPage.indexOf("/") == 0)
			return "";
		return otherPage;
	}

	// Remove duplicate links from the same source
	// IF TAKING TO LONG USE A HASH SET INSTEAD!
	private String checkForDuplicates(String otherPage, String pages) {
		String[] eachPage = pages.split(" ");
		for (String p : eachPage) {
			if (p.toLowerCase().equals(otherPage.toLowerCase()))
				return "";
		}
		return otherPage;
	}

	private ArrayList<String> parseTitleAndText(String value) {
		String[] result = {"",""};
		Pattern titleText = Pattern.compile("<title[^>]*>(.*)</title>");
		Pattern linksText = Pattern.compile("<text[^>]*>(.*)</text>",Pattern.DOTALL);
		Pattern linkPattern = Pattern.compile("\\[\\[([^\\[]*?)\\]\\]");
		
		String titleStr = "";
		Matcher title = titleText.matcher(value);
		while (title.find()) {
			result[0] = title.group(1);
			titleStr = title.group(1);
		}
		
		titleStr = titleStr.replace(" ", "_");
		
		String textBlob = "";
		Matcher linksTextMatcher = linksText.matcher(value);
		while (linksTextMatcher.find()) {
			textBlob = linksTextMatcher.group(1);
		}
		
		ArrayList<String> links = new ArrayList<String>();
		links.add(titleStr);
		Matcher linkMatcher = linkPattern.matcher(textBlob);
		while (linkMatcher.find()) {
			links.add(linkMatcher.group(1));
			
		}
		
		for (String s: links) {
			System.out.println(s);
		}
		
		
		return links;
	}
 }
