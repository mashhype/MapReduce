package bdpuh.mapreduce_A4;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MoviesRatings {
    
    public static void main(String args[]) throws InterruptedException {
        
        Job mapreduceA4 = null;
        Configuration conf = new Configuration();
        
        try {
            mapreduceA4 = Job.getInstance(conf, "mapreduceA4");
        
        } catch (IOException ex) {
            Logger.getLogger(MoviesRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
         try {
            FileInputFormat.addInputPath(mapreduceA4, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(MoviesRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
                
//        Path path = new Path(args[0]);
//        if ("u.item".equals(path.getName())) {
//            MultipleInputs.addInputPath(mapreduceA4, path, TextInputFormat.class, UitemMapper.class);
//            System.out.println("\n I just read the file named: " + path.getName() + "\n");
//        }
//        else
//            MultipleInputs.addInputPath(mapreduceA4, path, TextInputFormat.class, AverageRatingMapper.class);
        
        //Set the Mapper and Reducer class
        //mapreduceA4.setMapperClass(UitemMapper.class); 
        mapreduceA4.setMapperClass(AverageRatingMapper.class);
        mapreduceA4.setReducerClass(AverageRatingReducer.class);
        
        //Set the Jar File
        mapreduceA4.setJarByClass(bdpuh.mapreduce_A4.MoviesRatings.class);
        
        //Set the output path
        FileOutputFormat.setOutputPath(mapreduceA4, new Path(args[1]));
        
        //Set the output data format
        mapreduceA4.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the Output Key and Value Class
        mapreduceA4.setOutputKeyClass(Text.class);
        mapreduceA4.setOutputValueClass(RatingAveragerPair.class);
        
        try {
            mapreduceA4.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MoviesRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MoviesRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}
