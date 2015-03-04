
package bdpuh.MoviesAndRatings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {
    
    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();
    
    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0) {
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }
    
    public static TaggedKey read(DataInput in) throws IOException {
        TaggedKey taggedKey = new TaggedKey();
        taggedKey.readFields(in);
        return taggedKey;
    }
    public Text getJoinKey() {
       return joinKey;
    }

    public IntWritable getTag() {
        return tag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        tag.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        tag.write(out);
    }

    void set(String key, int tag) {
        this.joinKey.set(key);
        this.tag.set(tag);
    }
    
}
