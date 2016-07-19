/*

 only one input which is Title.ID file
 much smaller ID-Frequency file will be provided as a distributed cache

 One output:
 - Joined output of an inputfile and ID-Frequency file

*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.*;
import java.util.*;
import java.net.URI;

public class JoinIDTitle2
{
    public static void main(String[] args) throws Exception {

      Job pass = new Job();
      Configuration conf = pass.getConfiguration();

      String titleDocID = args[0]; // main 함수의 첫번째 인자
      String docIDFreq = args[1]; // main 함수의 두번째 인자
      String outputDir = args[2]; // 출력 디렉토리

      if (outputDir == null || titleDocID == null || docIDFreq == null) {
        throw new IllegalArgumentException("Missing Parameters");
      }
      
      // 여기다가 캐시할 파일 삽입
      DistributedCache.addCacheFile(new URI(docIDFreq), conf);

      pass.setJobName("Join ID and Title v2");
      pass.setJarByClass(JoinIDTitle2.class);

      pass.setOutputKeyClass(Text.class);
      pass.setOutputValueClass(Text.class);

      pass.setMapperClass(MyMapper.class);
      pass.setReducerClass(Reducer.class);
      
       //리듀스 태스크를 아예 없이 구현가능하다.
      pass.setNumReduceTasks(0);

      pass.setInputFormatClass(KeyValueTextInputFormat.class);
      FileInputFormat.addInputPath(pass, new Path(titleDocID));
      FileOutputFormat.setOutputPath(pass, new Path(outputDir));

      pass.waitForCompletion(true);
    }

    // title + docID input file handling
    // tag this with 1
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

      private HashMap<String, String> id_freq_map = new HashMap<String, String>();
      private Path[] localFiles;

      @Override
      public void setup(Context context) {
        try {
          localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());  
          FileInputStream fstream = new FileInputStream(localFiles[0].toString());
          // Get the object of DataInputStream
          DataInputStream in = new DataInputStream(fstream);
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          String strLine;
          // Read File Line By Line
          while ((strLine = br.readLine()) != null)   {
            System.out.println(strLine);
            String []tokens = strLine.split("\\t");
            id_freq_map.put(tokens[0], tokens[1]); // ID and Frequency
          }
          // Close the input stream
          in.close();
        }
        catch(Exception e) {
          e.printStackTrace();
          System.out.println("read from distributed cache: an exception!");
        }
      }
      @Override
      protected void map(Text key, Text value, final Context context) throws IOException, InterruptedException {
        if (id_freq_map.get(value.toString()) != null) {
          context.write(key, new Text(value + "\t" + id_freq_map.get(value.toString())));
          context.getCounter("Stats", "Number of Title+DocID").increment(1);
        }
      }
    }

}
