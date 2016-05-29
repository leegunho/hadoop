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

public class InvertedIndex3 {
        
 public static class Map extends Mapper<Text, Text, WordID, Text> {
    private WordID wordID = new WordID();
    
    public void map(Text docID, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n\f |,!#\"$.'%&=+-_^@`~:?<>(){}[];*/");

        while (tokenizer.hasMoreTokens()) {
          wordID.setWord(tokenizer.nextToken().toLowerCase());
          try {
            wordID.setDocID(Long.parseLong(docID.toString()));
          	}
          catch (Exception e) {
        	  // docID가 정수형으로 이루어지지 않은경우에 대한 케이스 카운트
            context.getCounter("Error", "DocID conversion error").increment(1);
            continue;
          }
          // 하나의 docID에 여러 word를 갖는 문장을 여러번 출력
          context.write(wordID, docID);
        }

    }
 } 
        
 public static class Reduce extends Reducer<WordID, Text, Text, Text> {
	 // Map.class 에서 출력된 레코드들 (정렬이 이미 되어있다.)
    public void reduce(WordID key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    
      try {
        String word = key.getWord();
        StringBuilder toReturn = new StringBuilder();
        boolean first = true;
        String prevDocID = "";
        
         // 중복된 docId를 제거한다.
        for (Text val : values) {
          String curDocID = val.toString();         	// 현재 docId
          if (!curDocID.equals(prevDocID)) {
            if (!first)
              toReturn.append(",");
            else
              first = false;
            
            toReturn.append(val.toString());			// 중복된 docId를 제거한다.
            prevDocID = curDocID;
          }
        }
        context.write(new Text(word), new Text(toReturn.toString()));
      }
      catch(Exception e) {
    	  // 리듀서 작업의 에러를 카운트
        context.getCounter("Error", "Reducer Exception:" + key.toString()).increment(1);
      }
    }
 }
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf, "Inverted Index 3");

    job.setJarByClass(InvertedIndex3.class);

    // if mapper outputs are different, call setMapOutputKeyClass and setMapOutputValueClass
    job.setMapOutputKeyClass(WordID.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // following 3 classes need to be top level classes because they are going to be created thru reflection class internally
    // also they need to have no parameter constructor
    job.setPartitionerClass(WordIDPartitioner.class);
    job.setGroupingComparatorClass(WordIDGroupingComparator.class); //wordId.getWord를 비교
    job.setSortComparatorClass(WordIDSortComparator.class); //wordId 객체자체를 비교, 같은 단어에 대한 docId를 비교

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(10);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
