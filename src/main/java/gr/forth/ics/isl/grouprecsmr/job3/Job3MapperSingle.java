package gr.forth.ics.isl.grouprecsmr.job3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job3MapperSingle extends MapReduceBase implements Mapper<Text,DoubleWritable,IntWritable,DoubleWritable> {
    
    IntWritable outputKey = new IntWritable();
    
    @Override
    public void map(Text key, DoubleWritable value,
            OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        
        Integer nonMember = Integer.parseInt(key.toString().split("_")[1]);
        outputKey.set(nonMember);
        output.collect(outputKey, value);        
    }
    
}
