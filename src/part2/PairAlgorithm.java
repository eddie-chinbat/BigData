/******************************************************************
* 
* @SID      610575
* @Author   Erdenebayar Chinbat
* @Created  Nov/12/2019
*
******************************************************************/

package part2;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairAlgorithm {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
     
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "PairAlgorithm");
        job.setJarByClass(PairAlgorithm.class);
         
        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
         
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        job.waitForCompletion(true);
    }
    
    //Beginning of Mapper class
    public static class MyMapper extends Mapper<LongWritable, Text, PairKey, IntWritable> {
        private Map<PairKey, Integer> myHash = new HashMap<PairKey, Integer>();
        private Logger log = Logger.getLogger(MyMapper.class);
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            String line = value.toString();
            String[] s = line.split("\\s");
            
            for(int i = 0; i < s.length; ++i) {
                Text word = new Text();
                word.set(s[i]);
                
                for(int j = i + 1; j < s.length; ++j) {
                    PairKey textKey = new PairKey(word, new Text(s[j]));
                    PairKey textVal = new PairKey(word, new Text("*"));
                    
                    if(s[i].equals(s[j])) break;
                    
                    if(myHash.containsKey(textKey)) myHash.put(textKey, myHash.get(textKey) + 1);
                    else myHash.put(textKey, 1);
                    
                    if(myHash.containsKey(textVal)) myHash.put(textVal, myHash.get(textVal) + 1);
                    else myHash.put(textVal, 1); 
                }
            }
        }
        
        @Override
        protected void cleanup(Mapper<LongWritable, Text, PairKey, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Map.Entry<PairKey, Integer> entry : myHash.entrySet())  {
                log.info("(" + entry.getKey() + ", " + entry.getValue() + ")");
                context.write(entry.getKey(), new IntWritable(entry.getValue())); 
            }
            super.cleanup(context);
        }
    } //End of Mapper class

    //Beginning of Reduce class
    public static class MyReduce extends Reducer<PairKey, IntWritable, PairKey, FloatWritable> {
        private float total = 0.0f;
        public void reduce(PairKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (IntWritable val : values)
                sum += val.get(); 
            
            if(key.getSecond().equals(new Text("*"))) total = sum;       //Add all values in H
            else context.write(key, new FloatWritable(sum / total));
        }
    } //End of Reduce class
}

class PairKey implements WritableComparable<PairKey> {
    private Text first;
    private Text second;
    
    public PairKey() {
        set(new Text(), new Text());
    }

    public PairKey(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public PairKey(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode() * 163;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PairKey) {
            PairKey tp = (PairKey) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
      return "(" + first + ", " + second + ")";
    }

    @Override
    public int compareTo(PairKey tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) return cmp;
        
        return second.compareTo(tp.second);
    }
}