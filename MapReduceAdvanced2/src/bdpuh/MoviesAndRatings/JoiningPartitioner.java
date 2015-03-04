
package bdpuh.MoviesAndRatings;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;


public class JoiningPartitioner extends Partitioner<TaggedKey, Text >{
    
    @Override
    public int getPartition(TaggedKey key, Text text, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
    
}
