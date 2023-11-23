import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.commonC

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Hashtable;
import java.io.FileReader;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {
    private Hashtable<String, String> joinMap = new Hashtable<String, String>();

    // 맵 출력기
    private Text outputKey = new Text();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        try{
            // 분산 캐시 조회
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            // 조인 데이터 생성
            if (cacheFiles != null && cacheFiles.length > 0) {
                String line;
                BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                try{
                    while ((line = br.readLine() != null)) {
                        CarrierCodeParser codeParser = new CarrierCodeParser(line);
                    }
                }
            }
        }
    }
}
