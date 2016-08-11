/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.forth.ics.isl.grouprecsmr.job3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author VASILIS
 */
public class Job3MapperMulti extends MapReduceBase implements Mapper<VIntWritable,Text,VIntWritable,Text> {
    
    IntWritable outputKey = new IntWritable();
    
    
    private Path[] localFiles;
    private Set<Integer> friends;
    
    @Override
    public void configure(JobConf conf) {
        friends = new HashSet<>();
        BufferedReader SW = null;
        try {
            localFiles = DistributedCache.getLocalCacheFiles(conf);
            Path groupFile = localFiles[0];
            SW = new BufferedReader(new FileReader(groupFile.toString()));
            String line;
            
            for (int i = 1; i < localFiles.length; ++i) {
                Path localFile = localFiles[i];
                SW = new BufferedReader(new FileReader(localFile.toString()));
                while ((line = SW.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String[] users = parts[0].split("_"); 
                    friends.add(Integer.parseInt(users[1])); //get the second part of the key (this is a friend)
                }
            }
            SW.close();
        } catch (FileNotFoundException e) {
                System.err.println(e.toString());
        } catch (IOException e) {
                System.err.println(e.toString());
        }
    }
    
    @Override
    public void map(VIntWritable key, Text value,
            OutputCollector<VIntWritable, Text> output, Reporter reporter) throws IOException {
        
        Integer nonMember = Integer.parseInt(value.toString().split("_")[0]);
        if (friends.contains(nonMember)) { //filter out nonFriends' ratings, as they are ignored for the recommendations
            output.collect(key, value);  //just forward the input (for friends only)    
        }
    }
    
}
