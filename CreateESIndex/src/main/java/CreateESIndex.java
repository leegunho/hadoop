import java.util.*;
import java.net.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CreateESIndex {
        
 public static class MyMapper extends Mapper<Text, Text, Text, Text> {
    private String baseUrl = "";

    @Override
    public void setup(Context context) {
      String []hosts = context.getConfiguration().getStrings("ESServer", "localhost");        
      baseUrl = "http://" + hosts[0] + ":9200/wikipedia/doc/";   
    }

    @Override
    public void map(Text docID, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // poor man's JSON escaping
        line = line.replace("\\","\\\\");
        line = line.replace("\"" , "\\\"");

        URL url = new URL(baseUrl + docID.toString());
        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        httpCon.setDoOutput(true); // this is needed when request method is PUT or POST
        httpCon.setRequestMethod("PUT");

        // Write Request content
        String content = "{ \"body\" : \" " + line + "\" }";
        OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
        out.write(content);
        out.close();

        // Read Response
        String inputLine = "", inputLines = "";
        BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
        while ((inputLine = in.readLine()) != null) {
          inputLines += inputLine;
        }

        // Response check
        if (inputLines.indexOf("\"ok\":true") < 0) {
          context.getCounter("Stats", "Error Docs").increment(1); // 성공응답을 받지못한경우
        }
        else {
          context.getCounter("Stats", "Success").increment(1);
        }
        in.close();
        httpCon.disconnect();
    }
 } 
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf, "CreateESIndex");

    job.setJarByClass(CreateESIndex.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(MyMapper.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(0);
    
    //엘라스틱서치 서버 주소
    job.getConfiguration().setStrings("ESServer", args[2]);        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
