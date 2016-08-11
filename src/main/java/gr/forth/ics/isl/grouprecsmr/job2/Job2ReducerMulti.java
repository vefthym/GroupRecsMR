package gr.forth.ics.isl.grouprecsmr.job2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job2ReducerMulti extends MapReduceBase implements Reducer<Text,ByteWritable,Text,DoubleWritable>{
    
    private double thresh;
    DoubleWritable outputSim = new DoubleWritable();
    
    @Override
    public void configure(JobConf conf) {
        thresh = conf.getFloat("friendsSimThreshold", 0.5f); //0.5 is the default value
    }
    
    
    @Override
    public void reduce(Text key, Iterator<ByteWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sum = 0;
        int numItems = 0;
        while (values.hasNext()) {
            numItems++;
            int partialSimUnsquared = values.next().get(); //byte converted to int
            sum += Math.pow(partialSimUnsquared,2); //square the partial distances
        }
        
        double similarity = 1 - (Math.sqrt(sum) / numItems);
        if (similarity > thresh) {
            outputSim.set(similarity);
            output.collect(key, outputSim);
        }
    }
    
}
