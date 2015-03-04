package bdpuh.mapreduce_A3;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RatingsCount {
    public static void main(String args[]) throws InterruptedException {
        Job ratingsCountJob = null;
        
        Configuration conf = new Configuration();
        try {
            //ratingsCountJob = new Job(conf, "RatingsCount");
            ratingsCountJob = Job.getInstance(conf, "RatingsCount");
        
        } catch (IOException ex) {
            Logger.getLogger(RatingsCount.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        //Specify the input path
        try {
            FileInputFormat.addInputPath(ratingsCountJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(RatingsCount.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        //Set the Input Data Format
        ratingsCountJob.setInputFormatClass(TextInputFormat.class);
        
        //Set the Mapper and Reducer class
        ratingsCountJob.setMapperClass(RatingsCountMapper.class);
        ratingsCountJob.setReducerClass(RatingsCountReducer.class);
        
        //Set the Jar File
        ratingsCountJob.setJarByClass(bdpuh.mapreduce_A3.RatingsCount.class);
        
        //Set the output path
        FileOutputFormat.setOutputPath(ratingsCountJob, new Path(args[1]));
        
        //Set the output data format
        ratingsCountJob.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the Output Key and Value Class
        ratingsCountJob.setOutputKeyClass(Text.class);
        ratingsCountJob.setOutputValueClass(IntWritable.class);
        
        try {
            ratingsCountJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingsCount.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingsCount.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
    }
}
