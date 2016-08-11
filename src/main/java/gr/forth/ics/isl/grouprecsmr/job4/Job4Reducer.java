/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.forth.ics.isl.grouprecsmr.job4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author VASILIS
 */
public class Job4Reducer extends MapReduceBase implements Reducer<VIntWritable,Text,VIntWritable,DoubleWritable>{
    
    private Path[] localFiles;
    Set<Integer> group;
    private Map<Integer,Double> similarities; //<nonMember,similarity> 
    
    @Override
    public void configure(JobConf conf) {
        similarities = new HashMap<>();
        group = new HashSet<>();
        
        BufferedReader SW = null;
        try {
            localFiles = DistributedCache.getLocalCacheFiles(conf);
            Path groupFile = localFiles[0];
            SW = new BufferedReader(new FileReader(groupFile.toString()));
            String line;
            while ((line = SW.readLine()) != null) {
                String groupLine = line;
                String[] groupMembers = groupLine.split(",");
                for (String groupMember : groupMembers) {
                    group.add(Integer.parseInt(groupMember));
                }
            }
            
            SW.close();
            for (int i = 1; i < localFiles.length; ++i) {
                Path localFile = localFiles[i];
                SW = new BufferedReader(new FileReader(localFile.toString()));
                while ((line = SW.readLine()) != null) {
                    String[] parts = line.split("\t");
                    similarities.put(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
                }
            }
            SW.close();
        } catch (FileNotFoundException e) {
                System.err.println(e.toString());
        } catch (IOException e) {
                System.err.println(e.toString());
        }
    }
    
    
    DoubleWritable outputScore = new DoubleWritable();
    
    /**
     * 
     * @param key movie id
     * @param values list of (nonMember_rating) values 
     * @param output key:movie id value:final outputScore
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(VIntWritable key, Iterator<Text> values,
			OutputCollector<VIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {        
        
        double score = 0;
        double sumSimilarity = 0;     
        
        while (values.hasNext()) {
            String[] value = values.next().toString().split("_");
            Integer nonMember = Integer.parseInt(value[0]);
            Double rating = Double.parseDouble(value[1]);
            
            Double similarity = similarities.get(nonMember); 
            
            if (similarity != null) { //then, they are friends
                sumSimilarity += similarity;
                score += similarity * rating;
            }
            
        }
    
        score /= sumSimilarity; 

        outputScore.set(score); 
        output.collect(key, outputScore);
    }
    
}
