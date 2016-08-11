package gr.forth.ics.isl.grouprecsmr.job1;

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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class Job1Reducer extends MapReduceBase implements Reducer<VIntWritable,Text,Text,ByteWritable>{
    MultipleOutputs mos;
    Set<Integer> group;
    
    Text userPair = new Text();
    ByteWritable part = new ByteWritable();
    
    private Path[] localFiles;
    
    @Override
    public void configure(JobConf conf) {
        mos = new MultipleOutputs(conf);
        group = new HashSet<>();
        BufferedReader SW;
        try {
            localFiles = DistributedCache.getLocalCacheFiles(conf);
            SW = new BufferedReader(new FileReader(localFiles[0].toString()));
            String line;
            while ((line = SW.readLine()) != null) {
                String groupLine = line;
                String[] groupMembers = groupLine.split(",");
                for (String groupMember : groupMembers) {
                    group.add(Integer.parseInt(groupMember));
                }
            }
            SW.close();
        } catch (FileNotFoundException e) {
                System.err.println(e.toString());
        } catch (IOException e) {
                System.err.println(e.toString());
        }
    }
    
    /**
     * 
     * @param key movieID
     * @param values list of user_rating pairs
     * @param output key: memeber_nonMember, value: partialDistance unsquared
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(VIntWritable key, Iterator<Text> values,
			OutputCollector<Text, ByteWritable> output, Reporter reporter) throws IOException {
        
        Map<Integer,Byte> groupRatings = new HashMap<>();
        Map<Integer,Byte> nonGroupRatings = new HashMap<>();
        while (values.hasNext()) {
            Text value = values.next();
            String[] user_rating = value.toString().split("_");
            int user = Integer.parseInt(user_rating[0]); //userId
            byte rating = Byte.parseByte(user_rating[1]); //rating in [0,9]
            
            if (group.contains(user)) {
                groupRatings.put(user, rating); 
            } else {
                nonGroupRatings.put(user, rating); 
            }
            
            
            if (groupRatings.isEmpty()) { //then this movie was not rated by any member of the group
                for (Integer nonMember : nonGroupRatings.keySet()) {
                    mos.getCollector("candidateMovies", reporter).collect(key, value); //just forward the input. this is a candidate item for recommendation
                }
            } else {
                for (Integer member : groupRatings.keySet()) {
                    Byte memberRating = groupRatings.get(member);
                    for (Integer nonMember : nonGroupRatings.keySet()) {
                        Byte nonMemberRating = nonGroupRatings.get(nonMember);
                        Byte partialScoreUnSquared = (byte) (memberRating - nonMemberRating);
                        userPair.set(member+"_"+nonMember);
                        part.set(partialScoreUnSquared);
                        output.collect(userPair, part);
                    }                    
                }
            }
            
        }
    }
    
    @Override
    public void close() throws IOException {
        mos.close();
    }
}
