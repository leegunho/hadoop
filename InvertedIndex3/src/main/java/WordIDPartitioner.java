import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class WordIDPartitioner extends Partitioner<WordID, Text> {
  protected WordIDPartitioner() {
  }

  @Override
  public int getPartition(WordID key, Text val, int numPartitions) {
    return (key.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
