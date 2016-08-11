package gr.forth.ics.isl.grouprecsmr.job3;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job3ReducerSingle extends MapReduceBase implements Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
    
    
    private double thresh;
    DoubleWritable outputSim = new DoubleWritable();
    
    @Override
    public void configure(JobConf conf) {
        thresh = conf.getFloat("friendsSimThreshold", 0.5f);
    }
    
    /**
     * 
     * @param key a non-member user
     * @param values the similarities of this user to member users
     * @param output key: the non-member user, if his Aggregate (min) similarity is above the friendship sim threshold
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        
        double min = 1;
        while (values.hasNext()) {
            double sim = values.next().get();
            if (sim < min) { //check if all similarities are above the threshold, if not, this is not a friend
                if (sim < thresh) {
                    return; //stop here. not a friend
                }
                min = sim;
            }
        }
        outputSim.set(min);
        output.collect(key, outputSim);
    }
    
}
