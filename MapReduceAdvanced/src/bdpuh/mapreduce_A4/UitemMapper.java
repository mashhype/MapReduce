package bdpuh.mapreduce_A4;

import com.google.common.base.Joiner;
import org.apache.log4j.Logger;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class UitemMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    Logger logger = Logger.getLogger(UitemMapper.class);
    private int udataKey; 
    private String valuesWithoutKey,  fileTag="uItem~";
    private IntWritable myKey = new IntWritable();
    private final Text data = new Text();
    private Joiner joiner;
    //private Splitter splitter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
//        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
//        System.out.println ("in stdout" + context.getTaskAttemptID().toString() + " " + fileName);
        System.err.println ("in stderr" + context.getTaskAttemptID().toString());
        joiner = Joiner.on(", ");
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String splitLine[] = line.split("\\|");
        udataKey = Integer.parseInt(splitLine[0]);
        valuesWithoutKey = joiner.join(splitLine[1], splitLine[2], splitLine[4]);
        data.set(fileTag + valuesWithoutKey);
        myKey.set(udataKey);
        context.write(myKey, data);
      
    }
        
}

