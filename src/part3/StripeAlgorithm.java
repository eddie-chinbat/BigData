/******************************************************************
* 
* @SID      610575
* @Author   Erdenebayar Chinbat
* @Created  Nov/12/2019
*
******************************************************************/

package part3;

import java.util.*;
import java.util.Map.Entry;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StripeAlgorithm {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
     
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "StripeAlgorithm");
        job.setJarByClass(StripeAlgorithm.class);
         
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
         
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        job.waitForCompletion(true);
    }
    
    //Beginning of Mapper class
    public static class MyMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private Map<String, Map<String, Integer>> myHash = new HashMap<String, Map<String, Integer>>();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            String line = value.toString();
            String[] s = line.split("\\s");
            
            for(int i = 0; i < s.length; ++i) {
                String word = s[i];
                for(int j = i+1; j < s.length; ++j) {
                    String secondWord = s[j];
                    if(word.equals(secondWord))
                        break;
                    if(!myHash.containsKey(word)) {
                        myHash.put(word, new HashMap<String, Integer>());
                    }
                    Map<String, Integer> map = myHash.get(word);
                    if(map.containsKey(secondWord)) {
                        map.put(secondWord, map.get(secondWord)+1);
                    } else {
                        map.put(secondWord, 1);
                    }
                }
            }
        }
        
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, MapWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for(Map.Entry<String, Map<String, Integer>> map : myHash.entrySet()) {
                MapWritable mapWritable = new MapWritable();
                for(Map.Entry<String, Integer> k : map.getValue().entrySet())
                    mapWritable.put(new Text(k.getKey()), new IntWritable(k.getValue()));
                context.write(new Text(map.getKey()), mapWritable);
            }
            super.cleanup(context);
        }
    } //End of Mapper class

    //Beginning of Reduce class
    public static class MyReduce extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MyMap mw = new MyMap();
            float sum = 0;
            for (MapWritable val : values) {
                for(Entry<Writable, Writable> e : val.entrySet()) {
                    sum += ((IntWritable)e.getValue()).get();
                    if(!mw.containsKey(e.getKey())) mw.put(e.getKey(), e.getValue());
                }
            }
            
            for(Entry<Writable, Writable> e : mw.entrySet()) {
                float value = ((IntWritable)e.getValue()).get() / sum;
                e.setValue(new FloatWritable(value));
            }

            context.write(key, mw);
        }
    } //End of Reduce class
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
