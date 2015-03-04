package bdpuh.MoviesAndRatings;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FilePath {
   
    public static void main(String args[]) throws InterruptedException, IOException {
        
        Splitter splitter = Splitter.on('/');
        StringBuilder filePaths = new StringBuilder();
        Job filePath = null;
       
        Configuration conf = new Configuration();
        conf.set("keyIndex", "0");
        
        try {
            filePath = Job.getInstance(conf, "filePath");
            
        
        } catch (IOException ex) {
            Logger.getLogger(FilePath.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
         try {
            FileInputFormat.addInputPath(filePath, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(FilePath.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
         
         for (int i = 0; i < args.length - 1; i++) {
             String fileName = Iterables.getLast(splitter.split(args[i]));
             conf.set(fileName, Integer.toString(i + 1));
             filePaths.append(args[i]).append(",");
         }
       
        //Set the Mapper and Reducer class
        //mapreduceA4.setMapperClass(UitemMapper.class); 
        filePath.setMapperClass(FileMapper.class);
        
        filePath.setReducerClass(FileReducer.class);
        
        //Set the Jar File
        filePath.setJarByClass(bdpuh.MoviesAndRatings.FilePath.class);
        
        //Set the output path
        FileOutputFormat.setOutputPath(filePath, new Path(args[1]));
       
        //Set the output data format
        filePath.setOutputFormatClass(TextOutputFormat.class);
       
        
        //Set the Output Key and Value Class
        filePath.setOutputKeyClass(TaggedKey.class);
        filePath.setOutputValueClass(Text.class);
        
        filePath.setPartitionerClass(JoiningPartitioner.class);
        filePath.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        
        
        try {
            filePath.waitForCompletion(true);
           
        } catch (IOException ex) {
            Logger.getLogger(FilePath.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(FilePath.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


}
