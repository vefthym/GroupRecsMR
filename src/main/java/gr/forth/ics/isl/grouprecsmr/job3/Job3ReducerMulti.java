/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.forth.ics.isl.grouprecsmr.job3;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
public class Job3ReducerMulti extends MapReduceBase implements Reducer<VIntWritable,Text,VIntWritable,DoubleWritable>{
    
    private Path[] localFiles;
    Set<Integer> group;
    private Table<Integer,Integer,Double> similarities; //< member, <nonMember,similarity> > : a map with a map value
    
    @Override
    public void configure(JobConf conf) {
        similarities = HashBasedTable.create();
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
                    String[] users = parts[0].split("_");
                    similarities.put(Integer.parseInt(users[0]), Integer.parseInt(users[1]), Double.parseDouble(parts[1]));
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
     * @param values list of (nonMember_rating) values, where rating is scaled to an integer in [0,9]
     * @param output key:movie id value:final outputScore
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(VIntWritable key, Iterator<Text> values,
			OutputCollector<VIntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        Set<Double> scores = new TreeSet<>(); //first = min
        
        Map<Integer,Double> valuesCopy = new HashMap<>();
        while (values.hasNext()) {
            String[] value = values.next().toString().split("_");
            Integer nonMember = Integer.parseInt(value[0]);
            Integer rating = Integer.parseInt(value[1]);
            valuesCopy.put(nonMember, rating/9.0); //scale rating from [0,9] to [0,1]
        }
        
        for (Integer member : group) { //for each user of the group
            double score = 0;
            double sumSimilarity = 0;     
            
            for (Integer nonMember : valuesCopy.keySet()) { //for each non member of the group who has rated this movie                              
                double rating = valuesCopy.get(nonMember); //the rating that the non group member gave to this movie
            
                Double similarity = similarities.get(member, nonMember);
                if (similarity != null) { //then, they are friends
                    sumSimilarity += similarity;
                    score += similarity * rating;
                }
            }
            score /= sumSimilarity;            
            scores.add(score); //this is the relevance score of this movie for the current member
        }
        
        outputScore.set(scores.iterator().next()); //first = min (treeset) //least measery strategy
        output.collect(key, outputScore);
    }
    
}
