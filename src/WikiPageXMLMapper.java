import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class WikiPageXMLMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException {    
    	
        String[] titleAndText = parseTitleAndText(value.toString());
         
        String pageString = titleAndText[0];
        Text page = new Text(pageString.replace(' ', '_'));

        String[] parts = titleAndText[1].split("\\[\\[");

        String pages = "!@#$ ";
        for (int i = 1; i < parts.length; i++) {
        	int lastIndexBrackets = parts[i].lastIndexOf("]]");
        	// This checks and skips the first part of the outer link
        	if (lastIndexBrackets == -1)
        		continue;
        	
        	String insideLinkPlusExtra = parts[i].substring(0, lastIndexBrackets);
        	
        	int multipleClosingBrackets = insideLinkPlusExtra.indexOf("]]");
          
            String otherPage = insideLinkPlusExtra;

            if (multipleClosingBrackets != -1) {
            	otherPage = insideLinkPlusExtra.substring(0, multipleClosingBrackets);
            }
            	
        	otherPage = otherPage.split("\\|")[0];
        	otherPage = checkForDuplicates(otherPage, pages);
        	otherPage = (otherPage.indexOf(":") == -1) ? otherPage : "";
        	otherPage = (otherPage.indexOf("#") == -1) ? otherPage : "";
        	otherPage = checkForSubpageLinks(otherPage);
        	otherPage = checkForRedLink(otherPage);
        	
        	if (otherPage == "")
        		continue;
       
        	Text oP = new Text(otherPage.replace(' ', '_'));
        	pages += oP + " ";
        	
//        	System.out.println("inlink to first reducer");
//        	System.out.println(oP +" <-- " + page);
        	// taking each outlink and making it its own key (ingraph)
        	try {
				output.write(new Text(oP), new Text(page));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        
        System.out.println(pages.equals("!@#$ "));
        
        boolean noOutlinks = pages.equals("!@#$ ");
        // Designate this page as not a redlink
        try {
			output.write(new Text(page), new Text("!@#$"));
			// This will ensure that we dont lose the node when we return from ingraph
			if (noOutlinks)
				output.write(new Text(page), new Text("$#@!"));
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        
        System.out.println("Title link");
        System.out.println(page + " -> !@#$");
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

	private String checkForRedLink(String otherPage) {
		// TODO Auto-generated method stub
		boolean error = false;
		
		if (error)
			otherPage = "";
		
		return otherPage;
	}

	private String[] parseTitleAndText(String value) {
		String[] result = {"",""};
		Pattern titleText = Pattern.compile("<title[^>]*>(.*)</title>");
		Pattern linksText = Pattern.compile("<text[^>]*>(.*)</text>",Pattern.DOTALL);
				
		Matcher title = titleText.matcher(value);
		while (title.find()) {
			result[0] = title.group(1);
		}
		
		Matcher linksTextMatcher = linksText.matcher(value);
		while (linksTextMatcher.find()) {
			result[1] = linksTextMatcher.group(1);
		}
		
		return result;
	}
 }
