/*
 Inverted Index: 
 - The map function parses each document, and emits a sequence of <word, document ID> pairs. 
 - The reduce function accepts all pairs for a given word, sorts the corresponding document IDs and emits a <word, list(document ID)> pair. 
 - The set of all output pairs forms a simple inverted index. It is easy to augment this computation to keep track of word positions.

 - Default heap size wouldn't work so need to increase it but in local mode there is only JVM so need to run "export HADOOP_HEAPSIZE=2048"
 - Or you can change the value of "mapred.child.java.opts" 
     <property>
         <name>mapred.child.java.opts</name>
         <value>-Xmx1024m</value>
     </property>
*/
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
        
public class InvertedIndex2 {
        
 public static class Map extends Mapper<Text, Text, Text, Text> {
    private Text word = new Text();
    HashSet<String> words = new HashSet<String>();
    
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n\f |,!#\"$.'%&=+-_^@`~:?<>(){}[];*/");

        words.clear();
        while (tokenizer.hasMoreTokens()) {
          words.add(tokenizer.nextToken().toLowerCase());
        }

        Iterator it = words.iterator();
        while(it.hasNext()) {
          String v =(String)it.next();
          word.set(v);
          context.write(word, key);
        }

    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

      try {
        StringBuilder toReturn = new StringBuilder();
        boolean first = true;

        for (Text val : values) {
          if (!first)
            toReturn.append(",");
          else
            first = false;

          toReturn.append(val.toString());
        }
        context.write(key, new Text(toReturn.toString()));
      }
      catch(Exception e) {
        context.getCounter("Error", "Reducer Exception:" + key.toString()).increment(1);
      }
     
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Inverted Index 2");

    // if mapper outputs are different, call setMapOutputKeyClass and setMapOutputValueClass
    job.setJarByClass(InvertedIndex2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(10);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
