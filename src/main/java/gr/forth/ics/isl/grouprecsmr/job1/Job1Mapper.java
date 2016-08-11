package gr.forth.ics.isl.grouprecsmr.job1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, VIntWritable, Text> {
    
    VIntWritable movieId = new VIntWritable();
    Text user_rating = new Text();
    
    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {
        
        String[] parts = value.toString().split("\t");
        if (parts.length == 1) {
            parts = value.toString().split("::"); //for the 10M file, another delimiter is used
        }
        String user = parts[0];
        int movie = Integer.parseInt(parts[1]);
//        double rating = (Double.parseDouble(parts[2])-1)/4.0; //scale ratings from {1,2,3,4,5} to [0,1]
        //initial rating is in {1, 1.5, 2, 2.5, ..., 5}. Make it an integer in [0,9] by multiplying with 2
        //this will save a lot of space by using VIntWritables, instead of DoubleWritables
        int rating = new Double((Double.parseDouble(parts[2])-1) * 2).intValue(); //rating *2 should be an int, since rating is in {1,1.5,2,2.5,...,5}
        
        movieId.set(movie);
        user_rating.set(user+"_"+rating);
        output.collect(movieId, user_rating);
    }
    
}
