import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class WordIDGroupingComparator extends WritableComparator {
  protected WordIDGroupingComparator() {
    super(WordID.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {

    // split to get natural key
    WordID k1 = (WordID)w1;
    WordID k2 = (WordID)w2;

    return k1.getWord().compareTo(k2.getWord());
  }
}

