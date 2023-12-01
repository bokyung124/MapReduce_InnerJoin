import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.List;
import java.util.Arrays;
import static java.util.stream.Collectors.toList;

//SPARK SQL GUIDE
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App {
    /**
     * @param args
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void main( String[] args ) throws FileNotFoundException, IOException
    {
        //spark builder name build
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark")
                .config("spark.master", "local")
                .config("spark.sql.files.maxPartitionBytes", 474262250) // (5.3GB / 4)
                .config("spark.sql.shuffle.partitions", 4)
                .getOrCreate();


        String rootPath = System.getProperty("user.dir");

        /* read CSV */
        Dataset<Row> leftDf = spark.read().format("csv").option("header","true").option("encoding", "UTF-8").load(rootPath + "/src/main/resources/fileA_4.csv");
        Dataset<Row> rightDf = spark.read().option("header","true").option("encoding", "UTF-8").csv(rootPath + "/src/main/resources/fileB_4.csv");

        List<String> leftCol = Arrays.asList(leftDf.columns());
        List<String> rightCol = Arrays.asList(rightDf.columns());
        String num = rightCol.get(rightCol.size()-1); // 임시일련번호 마지막 컬럼
        // System.out.println(num);

        Dataset<Row> leftKey = spark.read().option("header","true").option("encoding", "UTF-8").csv(rootPath+"/src/main/resources/joinA_4.csv"); // 결합 키 파일 읽기
        Dataset<Row> rightKey = spark.read().option("header","true").option("encoding", "UTF-8").csv(rootPath+"/src/main/resources/joinB_4.csv");

        /* (key 제외) 중복컬럼 있으면 이름 바꾸기 */
        List<String> duplicated = rightCol.stream().filter(x -> !x.equals(num) && !(String.format("`%s`", x)).equals(num))
                .filter(x -> leftCol.contains(x)).collect(toList());
        for(int i = 0; i < rightCol.size(); i++) {
            String col = rightCol.get(i);
            if (duplicated.contains(col)) {
                rightCol.set(i, col + "_B");
            }
        }

        /* read CSV with new colname */
        String[] newCol = rightCol.toArray(new String[0]);
        rightDf = spark.read().option("header","false").option("encoding", "UTF-8").csv(rootPath+"/src/main/resources/fileB_4.csv").toDF(newCol);
        leftDf = leftDf.withColumnRenamed("임시일련번호", "일련번호A");
        leftKey = leftKey.withColumnRenamed("임시일련번호", "일련번호A");
        rightDf = rightDf.withColumnRenamed("임시일련번호", "일련번호B");
        rightKey = rightKey.withColumnRenamed("임시일련번호","일련번호B");

        /* create View */
        leftDf.createOrReplaceTempView("A");
        rightDf.createOrReplaceTempView("B");
        leftKey.createOrReplaceTempView("keyA");
        rightKey.createOrReplaceTempView("keyB");

        /* query */
        String query = String.format("SELECT * FROM (SELECT * FROM A INNER JOIN keyA USING(`일련번호A`)) INNER JOIN (SELECT * FROM B INNER JOIN keyB USING (`일련번호B`)) USING(`결합키`);");

        Dataset<Row> result = spark.sql(query);

        // result.show(1);

        result.write().option("header","true").csv(rootPath+"/result");

        spark.stop();
    }
}
