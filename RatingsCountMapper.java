package bdpuh.mapreduce_A3;

import org.apache.log4j.Logger;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;



public class RatingsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    Logger logger = Logger.getLogger(RatingsCountMapper.class);
    private IntWritable one = new IntWritable();
    private Text word = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout" + context.getTaskAttemptID().toString() + " " + fileName);
        System.err.println ("in stderr" + context.getTaskAttemptID().toString());
        
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String line = value.toString();
//        line = line.replace("[,.:?\"!\\/]", "");
//        StringTokenizer tokenizer = new StringTokenizer(line);
//        
//        while (tokenizer.hasMoreTokens()) {
//            word.set(tokenizer.nextToken());
//            context.write(word, one);
        String[] split = value.toString().split("\t+");
        word.set(split[2]);
        one.set(Integer.parseInt(split[2]));
        context.write(word, one);
    }
        
}
    