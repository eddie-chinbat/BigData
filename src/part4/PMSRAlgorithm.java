/******************************************************************
* 
* @SID      610575
* @Author   Erdenebayar Chinbat
* @Created  Nov/12/2019
*
******************************************************************/

package part4;

import java.util.*;
import java.util.Map.Entry;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PMSRAlgorithm {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
     
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "PMSRAlgorithm");
        job.setJarByClass(PMSRAlgorithm.class);
         
        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMap.class);
         
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
                context.write(entry.getKey(), new IntWritable(entry.getValue())); 
            }
            super.cleanup(context);
        }
    } //End of Mapper class

    //Beginning of Reduce class
// Pseudo code that Professor wrote on board today    
//    Class Reducer
//    method Initialize
//        uprev <- 0, H = new AssociatedArray
//    method reduce((u,v), [c1,c2,...])
//        sum = 0
//        for all c in [c1,c2,...] dp
//            sum += c
//
//        if(uprev <> u and uprev <> 0)
//            total = Total(H)  // add all values in H
//            H <- H | total
//            Emit(uprev, H)
//            H <- AssociatedArray
//        H{v} <- sum
//        uprev <- u
//    method close
//        total = Total(H)
//        H <- H | total
//        Emit(uprev, H)
    public static class MyReduce extends Reducer<PairKey, IntWritable, Text, MyMap> {
        private Text uprev = null;
        private MyMap H = new MyMap();
        private int total = 0;
        
        public void reduce(PairKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if(!key.getFirst().equals(uprev) && uprev != null) {
                for(Entry<Writable, Writable> entry : H.entrySet()){
                    total += ((IntWritable)entry.getValue()).get();   
                }
                for(Entry<Writable, Writable> entry : H.entrySet()){
                    entry.setValue(new DoubleWritable(((IntWritable)entry.getValue()).get() / (double)total));
                }
//                if(!key.getFirst().equals(new Text("*")))
                context.write(uprev, H);
                H = new MyMap();
            }
            
            int sum = 0;
            
            for (IntWritable value : values) {
                sum += value.get();
            }
            H.put(new Text(key.getSecond()), new IntWritable(sum));
            uprev = new Text(key.getFirst());
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

class MyMap extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet)
            result.append("[" + key.toString() + ", " + this.get(key) + "]");
        
        return result.toString();
    }
}
