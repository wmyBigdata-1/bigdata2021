package wmy.hadoop.mapreduce.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/*
 *@description: 没有shuffle就没有数据倾斜
 *@author: 情深@骚明
 *@time: 2021/2/7 13:08
 *@Version 1.0
 */
public class mapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pMap = new HashMap<>();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        // 添加一个文件
        String path = cacheFiles[0].getPath().toString();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        // 使用这个的话就有编码的问题，处理起来的话就会比较困难
        // FSDataInputStream bufferedReader = fileSystem.open(new Path(path));

        // 开流: HDFS处理换行的非常差，有编码的问题，没有中文的话使用Hadoop流，否则开本地流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] fields = line.split("\t");
            // 01	小米
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        // 1001	01	1
        String pname = pMap.get(fields[1]);
        if (pname == null) {
            pname = "null";
        }
        k.set(fields[0] + "\t" + pname + "\t" + fields[2]);
        context.write(k, NullWritable.get());
    }
}