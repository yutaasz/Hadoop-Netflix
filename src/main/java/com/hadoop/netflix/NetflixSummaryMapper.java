package com.hadoop.netflix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NetflixSummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text("SUMMARY");
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (!line.isEmpty()) {
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }
}
