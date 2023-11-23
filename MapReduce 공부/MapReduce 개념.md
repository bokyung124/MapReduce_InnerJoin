[출처] "시작하세요! 하둡 프로그래밍", 정재화 저

## 1. 맵리듀스

- 하둡은 HDFS와 맵리듀스로 구성됨
- 맵: 입력 파일을 한 줄씩 읽어서 데이터를 변형시킴
    - 맵의 데이터 변형 규칙은 개발자가 자유롭게 정의할 수 있음
    - 한 줄에 하나의 데이터가 출력됨
- 리듀스: 맵의 결과 데이터 집계

### 일반적으로

```java
맵: (k1, v1) -> list(k2, v2)
리듀스: (k2, list(v2)) -> (k3, list(v3))
```

- 맵은 키(k1)와 값(v1)으로 구성된 데이터를 입력받아 이를 가공하고 분류한 후, 새로운 키(k2)와 값(v2)으로 된 목록 출력
- 맵이 반복적으로 수행되면 새로운 키(k2)를 가진 여러 개의 데이터가 만들어짐

- 리듀스는 새로운 키(k2)로 그룹핑된 값의 목록(list(v2))을 입력 데이터로 전달받음
- 값의 목록(list(v2))에 대한 집계 연산을 실행해 새로운 키(k3)로 그룹핑된 새로운 값의 목록(list(v3)) 생성

<br>

## 2. 맵리듀스 프로그래밍 요소

### 데이터 타입

- 네트워크 통신을 위한 최적화된 객체로 `WritableComparable` 인터페이스 제공
- 키와 값으로 사용되는 모든 데이터 타입은 반드시 `WritableComparable` 인터페이스가 구현되어 있어야 함

- WritableComparable.java
```java
public interface WritableComparable<T> extends Writable, Comparable<T> {}
```

- Writable.java
```java
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public interface Writable {
    // write: 메서드 값 직렬화
    void write(DataOutput out) throws IOException;

    // readFields: 직렬화된 데이터값을 해제해서 읽음
    void readFields(DataInput in) throws IOException;
}
```

<br>

### 직접 데이터 타입 정의

- MyWritableComparable.java
```java
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritableComparable implements WritableComparable {
    private int counter;
    private long timestamp;

    public void write(DataOutput out) throws IOException {
        out.writeInt(counter);
        out.writeLong(timestamp);
    }

    public void readFileds(DataInput in) throws IOException {
        counter = in.readInt();
        timestamp = in.readLong();
    }

    @Override
    public int compareTo(Object o) {
        MyWritableComparable w = (MyWritableComparable) o;
        if (counter > w.counter) {
            return -1;
        } else if (counter < w.counter) {
            return 1;
        } else {
            if (timestamp < w.timestamp) {
                return 1;
            } else if (timestamp > w.timestamp) {
                return -1;
            } else {
                return 0;
            }
            }
        }
    }
}
```

- int 타입과 long 타입 변수를 동시에 갖고 있는 데이터 타입

<br>

### InputFormat

```java
public abstract class InputFormat<K, V> {
    pblic abstract List<InputStplit> getSplits(JobContext context) 
        throws IOException, Interrupedte;

    public abstract RecordReader<K, V> createRecodrdReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException;
}
```

<br>

## 3. Mapper

- 매퍼 클래스 그대로 사용할 수도 있지만, 대부분 매퍼 클래스를 상속받아 새롭게 구현함

```java
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    // Context -> job 정보 얻기, 입력 스플릿 레코드 단위로 읽기
    public class Context extends MapContextM<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        public Context(Configuration conf, TaskAttemptID taskid,
                       RecordReader<KEYIN, VALUEIN> reader,
                       RecordWriter<KEYOUT, VALUEOUT> writer,
                       OutputCommitter committer,
                       StatusReporter reporter,
                       InputSplit split) throws IOException, InterruptedException  {
            super(conf, taskid, reader, writer, committer, reporter, split);
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        // NOTHING
    }

    protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        context.write((KEYOUT) key, (VALUEOUT) value);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // NOTHING
    }

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
```

<br>

## 4. Partitioner

- 맵 태스크의 출력 데이터가 어떤 리듀스 태스크로 전달될지 결정
- 기본 파티셔너로 `HashPartitioner` 제공

```java
public class HashPartitioner<K, V> extends Partitioner<K, V> {
    public int getPartition(K key, V value, int numReduceTasks) {
        return (key.hashCode()& Integer.MAX_VALUE) % numReduceTasks;
    }
}
```

- 파티션 번호 반환
- 맵 태스크의 출력키와 값, 전체 리듀스 태스크 개수를 파라미터로 받아 `hash(키) % 전체 리듀스 태스크 개수` 형태로 파티션 계산
- 계산된 결과대로 맵 태스크가 실행된 노드에 파티션이 생성된 후, 맵 태스크의 출력 데이터가 저장됨

<br>

## 5. Reducer

- 맵 태스크의 출력 데이터를 입력 데이터로 전달받아 집계 연산 수행

```java
public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public class Context extends ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        public Context(Configuration conf, TaskAttemptID taskid,
                       RawKeyValueIterator input,
                       Counter inputKeyCounter,
                       Counter inputValueCounter,
                       RecordWriter<KEYOUT, VALUEOUT> output,
                       OutputCommitter committer,
                       StatusReporter reporter,
                       RawComparator<KEYIN> comparator,
                       Class<KEYIN> keyClass,
                       Class<VALUEIN> valueClass) throws IOException, InterruptedException {
            super(conf, taskid, input, inputKeyCounter, inputValueCounter, output, comitter, reporter, comparator, keyClass, valueClass);
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {
        // NOTHNG
    }

    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
        for (VALUEIN value: values) {
            context.write((KEYOUT) key, (VALUEOUT value));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // NOTHING
    }

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (Context.nextKey()) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
```