package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class CSVJoin {
    public static class JoinMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text record = new Text();

        private boolean isHeader = true;
        private String header = "";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String tag = "";

            // 태그 설정
            String filename = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
            if (filename.contains("fileA")) {
                tag = "fileA";
            } else if (filename.contains("joinA")) {
                tag = "joinA";
            } else if (filename.contains("fileB")) {
                tag = "fileB";
            } else if (filename.contains("joinB")) {
                tag = "joinB";
            }

            // 첫 번째 레코드를 헤더로 설정
            if (isHeader) {
                String[] headerTokens = value.toString().split(",");
                header = tag + "," + String.join(",", headerTokens);
                isHeader = false;
                return;
            }

            String values = value.toString();

            if (tag.contains("A")) {
                if (tag.contains("file")) {
                    joinKey.set(new Text(tokens[0]));
                    record.set(tag + "," + values);
                    context.write(joinKey, record);
                } else if (tag.contains("join")) {
                    joinKey.set(new Text(tokens[0]));
                    record.set(tag + "," + tokens[1]);
                    context.write(joinKey, record);
                }
            } else if (tag.contains("B")) {
                if (tag.contains("file")) {
                    joinKey.set(new Text(tokens[tokens.length-1]));
                    record.set(tag + "," + values);
                    context.write(joinKey, record);
                } else if (tag.contains("join")) {
                    joinKey.set(new Text((tokens[0])));
                    record.set(tag + "," + tokens[1]);
                    context.write(joinKey, record);
                }
            }
        }
    }

    public static class JoinReducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> fileValues = new ArrayList<>();
            String joinValue = null;

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                String tag = tokens[0];

                if (tag.contains("file")) {
                    fileValues.add(value.toString());
                } else if (tag.contains("join")) {
                    joinValue = tokens[1];
                }
            }
            if (joinValue != null) {
                for (String fileValue : fileValues) {
                    context.write(new Text(joinValue), new Text(fileValue));
                }
            }
        }
    }

    public static class JoinMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text joinValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t", 2);

            joinKey.set(fields[0]);
            joinValue.set(fields[1]);
            context.write(joinKey, joinValue);
        }
    }

    public static class JoinReducer2 extends Reducer<Text, Text, NullWritable, Text> {
        private Text outputValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append(",");
            }
            outputValue.set(sb.toString().trim());
            context.write(NullWritable.get(), outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("yarn.app.mapreduce.am.resource.mb", "4096");

        conf.set("mapreduce.map.memory.mb", "4096");
        conf.set("mapreduce.reduce.memory.mb", "4096");

        conf.set("mapreduce.map.java.opts.max.heap", "3277");
        conf.set("mapreduce.reduce.java.opts.max.heap", "3277");
        conf.set("mapreduce.map.java.opts", "-Xmx3277m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx3277m");

        conf.set("mapreduce.compress.map.output", "true");
        conf.set("mapreduce.output.compression.type", "BLOCK");
        conf.set("mapreduce.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        conf.set("mapreduce.map.cpu.vcores", "3");
        conf.set("mapreduce.reduce.cpu.vcores", "3");

        conf.set("mapreduce.tasktracker.map.tasks.maximum", "4");
        conf.set("mapreduce.job.jvm.numtasks", "-1");

        Job job1 = Job.getInstance(conf, "Mapper1");
        job1.setJarByClass(CSVJoin.class);
        job1.setMapperClass(JoinMapper1.class);
        job1.setReducerClass(JoinReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_output1"));

        job1.waitForCompletion(true);

        Job job = Job.getInstance(conf, "Reducer3");
        job.setJarByClass(CSVJoin.class);
        job.setMapperClass(JoinMapper2.class);
        job.setReducerClass(JoinReducer2.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("temp_output1"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
