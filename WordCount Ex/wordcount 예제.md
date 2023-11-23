## 1. 하둡 설치

[install hadoop.md](https://github.com/bokyung124/MapReduce_Join/blob/main/install%20hadoop.md)

<br>

## 2. WordCount.java

- MapReduce

[WordCount.java](https://github.com/bokyung124/MapReduce_Join/blob/main/WordCount.java)

<br>

### library

```java
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
```

<br>

### Mapper

- 키와 값으로 구성된 입력 데이터를 전달받아 데이터를 가공하고 분류해 새로운 데이터 목록 생성
- 입력 split마다 하나의 Map task 생성 = Mapper class
- 4개 타입 명시 <입력키, 입력값, 출력키, 출력값>
- IntWritable -> int
    - Writable: 맵리듀스가 제공하는 네트워크 통신을 위한 최적화된 객체

- `one` 을 final static으로 선언한 이유: 맵 메서드에서 출력하는 단어의 글자 수가 무조건 1이기 때문

```java
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
```

- map(key: 라인번호, value: 라인)
- 파일의 각 라인을 토큰화해서 `context`에 각 토큰과 숫자 `1` 입력
- Context 객체
    - job에 대한 정보를 얻어오고
    - 입력 split을 레코드 단위로 읽을 수 있음
    - `map` 메서드가 키와 값 형태로 데이터를 읽을 수 있음
    - `run` 메서드가 호출될 때 `Context` 객체에 있는 키를 순회 -> `map` 메서드 호출 -> 정의되어 있는 내용대로 로직 수행 

<br>

### Reducer

- Map task의 출력 데이터를 입력 데이터로 전달받아 집계 연산 수행
- 4개 타입 명시 <입력키, 입력값, 출력키, 출력값>

```java
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
```

- reduce(key: 단어, value: 글자수 목록)
- 각 value에 대한 개수 context에 저장

<br>

- main 함수

```java
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
```

### Combiner

- Map task의 출력 데이터는 네트워크를 통해 Reduce task로 전달됨
- `셔플`: 이러한 Map task와 Reduce task 사이의 데이터 전달 과정
- Combiner class : 셔플할 데이터의 크기를 줄이는데 도움을 줌
    - 네트워크로 전달 -> 전송할 데이터의 크기가 작을수록 전체 job의 성능은 좋아질 것
- 로컬 노드에 출력 데이터를 생성한 후 Reduce task에 네트워크로 전달
- 이때 출력 데이터는 기존 Mapper의 출력 데이터보다 크기가 줄어듦 -> job 성능 향상

<br>

## 3. jar file 생성

```bash
# hadoop 폴더 안에 wordcount 폴더 생성
mkdir $HADOOP_HOME/wordcount
cd $HADOOP_HOME/wordcount

# WordCount.java 파일 복사
cp [WordCount.java 경로] .
```

```bash
hadoop com.sun.tools.javac.Main WordCount.java

ls
# WordCount$IntSumReducer.class WordCount$TokenizerMapper.class WordCount.class WordCount.java
```

```bash
jar cf wc.jar WordCount*.class

ls
# WordCount$IntSumReducer.class WordCount$TokenizerMapper.class WordCount.class WordCount.java wc.jar
```

<br>

## 4. 하둡에 txt 파일 업로드

```bash
hdfs dfs -mkdir -p /tmp/input
hdfs dfs -put [txt파일 경로] /tmp/input
```

<br>

## 5. jar file 실행

```bash
hadoop jar wc.jar WordCount /tmp/input /tmp/output
```