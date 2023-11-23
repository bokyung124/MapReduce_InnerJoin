import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        public final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // mapper
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            // 각 라인 읽어들여서 토큰화
            StringTokenizer itr = new StringTokenizer(value.toString());

            // 출력 객체에 해당하는 context에 각각의 token-value 값을 token - 1 값으로 매핑해서 넣기
            // 중복되는 토큰이 있으면 다른 토큰으로 취급하여 새롭게 넣음
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // reducer
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
                            ) throws IOException, InterruptedException {
            // 정렬이 끝난 상태에서의 리듀스 작업
            // 각각의 키에 대한 value 값들이 하나로 합쳐지고 쓰여짐
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        // mapper와 reducer 사이에 combiner 함수 실행됨
        // 중복된 키들의 value 값 하나로 모아줌
        // 이후에는 셔플, 파티셔닝, 정렬 과정을 거쳐 reducer로 넘어감
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
