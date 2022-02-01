import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class AverageCalculator {
    public static class PrimaryMap extends Mapper<LongWritable, Text, IntWritable, MapWritable> {
        Random random = new Random();
        private static final Logger logger = Logger.getLogger(AverageCalculator.PrimaryMap.class);
        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            int key = random.nextInt(4) + 1;
            IntWritable mapKey = new IntWritable(key);
            String[] numberList = text.toString().trim().split(",");
            for (String number : numberList) {
                sum += Integer.parseInt(number);
                count += 1;
                logger.info(Integer.toString(sum) +" "+ Integer.toString(count));
            }
            IntWritable sumIntWritable = new IntWritable(sum);
            IntWritable countIntWritable = new IntWritable(count);
            MapWritable mapWritable = new MapWritable();
            mapWritable.put(sumIntWritable, countIntWritable);
            context.write(mapKey, mapWritable);
        }
    }

    public static class PrimaryReduce extends Reducer<IntWritable, MapWritable, IntWritable, IntWritable> {
        private static final Logger logger = Logger.getLogger(AverageCalculator.PrimaryReduce.class);
        public void reduce(IntWritable intWritable, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (MapWritable val : values) {
                logger.info("iterator started");
                for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
                    sum += Integer.parseInt(entry.getKey().toString());
                    count += Integer.parseInt(entry.getValue().toString());
                    logger.info(Integer.toString(sum) +" "+ Integer.toString(count));
                }
            }
            context.write(new IntWritable(sum), new IntWritable(count));
        }
    }

    public static class SecondaryMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text one = new Text("One");

        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            context.write(one, text);
        }
    }

    public static class SecondaryReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            Integer count = 0;
            for (Text val : values) {
                String value = val.toString();
                String[] temp = value.split("\\s+");
                if (temp.length == 2) {
                    sum += Integer.valueOf(temp[0]);
                    count += Integer.valueOf(temp[1]);
                }
            }

            String avg = String.valueOf(sum.doubleValue() / count);
            context.write(new Text("output"), new Text(avg));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job 1");
        job1.setJarByClass(AverageCalculator.class);
        job1.setMapperClass(AverageCalculator.PrimaryMap.class);
        job1.setReducerClass(AverageCalculator.PrimaryReduce.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(MapWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job 2");
        job2.setJarByClass(AverageCalculator.class);
        job2.setMapperClass(AverageCalculator.SecondaryMap.class);
        job2.setReducerClass(AverageCalculator.SecondaryReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
