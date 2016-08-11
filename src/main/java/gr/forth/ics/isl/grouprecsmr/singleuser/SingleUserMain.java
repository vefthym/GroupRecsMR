package gr.forth.ics.isl.grouprecsmr.singleuser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class SingleUserMain {
    
    public static void main(String[] args) {
        //paths
        Path inputRatingsPath = new Path(args[0]); //movieid, userid, rating (text files)
        Path job1OutputPath = new Path("/user/hduser/partialResults"); 
        Path partialDistancesPath = new Path("/user/hduser/partialResults/part-*"); //member_nonMember \t partialDistance (sequence files)
        Path candidateMoviesPath = new Path("/user/hduser/partialResults/candidateMovies-*"); //candidateMovieId, nonMemberUserId_rating (text files)
        Path userSimilaritiesPath = new Path("/user/hduser/allSimilarities");
        Path friendsSimilaritiesPath = new Path("/user/hduser/userSimilarities"); //similarity of each group member to his friends (text files)
        Path finalScoresPath = new Path(args[1]); //movieId \t outputScore
        
        int numReduceTasks = 56; //defaultValue
        if (args.length == 3) {
            numReduceTasks = Integer.parseInt(args[2]);
        }
        
        final float friendsSimThresh = 0.8f;
        
        String groupFilePath = "/user/hduser/group.txt"; //one-line csv file with user ids (text file)
        
        if (args.length < 2 || args.length > 3) {
            System.err.println("Incorrect input. Example usage: hadoop jar ~/GroupRecs/SingleUser.jar inputPath outputPath [numReduceTasks]");
            return;
        }
        
        //JOB 1//
        JobClient client = new JobClient();
        JobConf conf = new JobConf(gr.forth.ics.isl.grouprecsmr.singleuser.SingleUserMain.class);
        
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(job1OutputPath)) {
                fs.delete(job1OutputPath, true);
            }
            if (fs.exists(userSimilaritiesPath)) {
                fs.delete(userSimilaritiesPath, true);
            }
            if (fs.exists(friendsSimilaritiesPath)) {
                fs.delete(friendsSimilaritiesPath, true);
            }
            if (fs.exists(finalScoresPath)) {
                fs.delete(finalScoresPath, true);
            }
        } catch (IOException ex) {
            Logger.getLogger(SingleUserMain.class.getName()).log(Level.SEVERE, null, ex);
        }

        conf.setJobName("Single-user approach - Job 1");
        System.out.println("Starting Job 1 (Single-user approach)...");

        conf.setMapOutputKeyClass(VIntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(ByteWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,	SequenceFile.CompressionType.BLOCK);

        FileInputFormat.setInputPaths(conf, inputRatingsPath); //user ratings
        FileOutputFormat.setOutputPath(conf, job1OutputPath); //partial distances
        
        MultipleOutputs.addNamedOutput(conf, "candidateMovies", SequenceFileOutputFormat.class,VIntWritable.class, Text.class); //movieId, userId_rating

        conf.setMapperClass(gr.forth.ics.isl.grouprecsmr.job1.Job1Mapper.class);		
        conf.setReducerClass(gr.forth.ics.isl.grouprecsmr.job1.Job1Reducer.class);

        conf.setNumReduceTasks(numReduceTasks);

        try {
            DistributedCache.addCacheFile(new URI(groupFilePath), conf); // group	
        } catch (URISyntaxException e1) {
            System.err.println(e1.toString());
        }
        
        conf.setInt("mapred.task.timeout", 6000000);
        
        client.setConf(conf);
        RunningJob job;
        try {
            job = JobClient.runJob(conf);	
            job.waitForCompletion();
        } catch (Exception e) {
            System.err.println(e);
        }
        
        
        
        //JOB 2//
        System.out.println("Starting Job 2 (Single-user approach)...");
        JobClient client2 = new JobClient();
        JobConf conf2 = new JobConf(gr.forth.ics.isl.grouprecsmr.singleuser.SingleUserMain.class);

        conf2.setJobName("Single-user approach - Job 2");
        
        conf2.setMapOutputKeyClass(Text.class);           //user pair (member_nonMember)
        conf2.setMapOutputValueClass(ByteWritable.class); //similarity part unsquared

        conf2.setOutputKeyClass(Text.class);            //user pair (member_nonMember), where nonMember is a friend or not a friend
        conf2.setOutputValueClass(DoubleWritable.class);//similarity

        conf2.setInputFormat(SequenceFileInputFormat.class);
        //conf2.setInputFormat(TextInputFormat.class);
//        conf2.setOutputFormat(TextOutputFormat.class);
        conf2.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf2, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.setInputPaths(conf2, partialDistancesPath); //Job 1 output
        FileOutputFormat.setOutputPath(conf2, userSimilaritiesPath); //Job 2 output (similarity of each group member to all non-members)

        conf2.setMapperClass(IdentityMapper.class);		
        conf2.setReducerClass(gr.forth.ics.isl.grouprecsmr.job2.Job2ReducerSingle.class);
        
        conf2.setNumReduceTasks(numReduceTasks);
        
        conf2.setInt("mapred.task.timeout", 6000000);
        conf2.set("io.sort.mb", "500");

        client2.setConf(conf2);
        RunningJob job2;
        try {
            job2 = JobClient.runJob(conf2);	
            job2.waitForCompletion();
        } catch (Exception e) {
            System.err.println(e);
        }
        
        
        //JOB 3
        
        System.out.println("Starting Job 3 (Single-user approach)...");
        JobClient client3 = new JobClient();
        JobConf conf3 = new JobConf(gr.forth.ics.isl.grouprecsmr.singleuser.SingleUserMain.class);

        conf3.setJobName("Single-user approach - Job 3");

        conf3.setOutputKeyClass(IntWritable.class);     //a nonMember, where nonMember is in friends
        conf3.setOutputValueClass(DoubleWritable.class);//similarity

        conf3.setInputFormat(SequenceFileInputFormat.class);
        //conf2.setInputFormat(TextInputFormat.class);
        conf3.setOutputFormat(TextOutputFormat.class);
        //conf2.setOutputFormat(SequenceFileOutputFormat.class);
        //SequenceFileOutputFormat.setOutputCompressionType(conf2, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.setInputPaths(conf3, userSimilaritiesPath); //Job 2 output
        FileOutputFormat.setOutputPath(conf3, friendsSimilaritiesPath); //Job 3 output (id of all friens, and their similarity)

        conf3.setMapperClass(gr.forth.ics.isl.grouprecsmr.job3.Job3MapperSingle.class);		
        conf3.setReducerClass(gr.forth.ics.isl.grouprecsmr.job3.Job3ReducerSingle.class);

        int numSimilaritiesPartitions = numReduceTasks;
        conf3.setNumReduceTasks(numSimilaritiesPartitions);
        
        conf3.setFloat("friendsSimThreshold", friendsSimThresh);
        
        conf3.setInt("mapred.task.timeout", 6000000);
        conf3.set("io.sort.mb", "500");

        client3.setConf(conf3);
        RunningJob job3;
        try {
            job3 = JobClient.runJob(conf3);	
            job3.waitForCompletion();
        } catch (Exception e) {
            System.err.println(e);
        }
        
                
        //JOB 4//
        System.out.println("Starting Job 4 (Single-user approach)...");
        JobClient client4 = new JobClient();
        JobConf conf4 = new JobConf(gr.forth.ics.isl.grouprecsmr.singleuser.SingleUserMain.class);

        conf4.setJobName("Single-user approach - Job 4");

        conf4.setMapOutputKeyClass(VIntWritable.class);
        conf4.setMapOutputValueClass(Text.class);
        
        
        conf4.setOutputKeyClass(VIntWritable.class);
        conf4.setOutputValueClass(DoubleWritable.class);

        conf4.setInputFormat(SequenceFileInputFormat.class);
        //conf3.setInputFormat(TextInputFormat.class);
        conf4.setOutputFormat(TextOutputFormat.class);
        //conf3.setOutputFormat(SequenceFileOutputFormat.class);
        //SequenceFileOutputFormat.setOutputCompressionType(conf3,SequenceFile.CompressionType.BLOCK);
        
        
        try {
            DistributedCache.addCacheFile(new URI(groupFilePath), conf4);
        } catch (URISyntaxException ex) {
            System.err.println("Could not add group file to distributed cache. "+ex);
        }
        for (int i = 0; i < numSimilaritiesPartitions; i++) {                        
            String reduceId = String.format("%05d", i); //5-digit int with leading
            try {
                DistributedCache.addCacheFile(new URI(friendsSimilaritiesPath.toString()+"/part-"+reduceId), conf4);
            } catch (URISyntaxException ex) {
                System.err.println("Could not add similarities files to distributed cache. "+ex);
            }
            
        }
        
        FileInputFormat.setInputPaths(conf4, candidateMoviesPath); //Job 1 output (candidate movies)
        FileOutputFormat.setOutputPath(conf4, finalScoresPath); //Job 3 output (movie \t outputScore)

//        conf4.setMapperClass(IdentityMapper.class);		
        conf4.setMapperClass(gr.forth.ics.isl.grouprecsmr.job4.Job4MapperSingle.class); //use multi-user approach's job3  mapper
        conf4.setReducerClass(gr.forth.ics.isl.grouprecsmr.job4.Job4Reducer.class);

        conf4.setNumReduceTasks(numReduceTasks);
        
        conf4.setInt("mapred.task.timeout", 6000000);
        conf4.set("io.sort.mb", "500");
       
        client4.setConf(conf4);
        RunningJob job4;
        try {
            job4 = JobClient.runJob(conf4);	
            job4.waitForCompletion();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
