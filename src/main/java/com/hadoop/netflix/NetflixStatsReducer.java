package com.hadoop.netflix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NetflixStatsReducer extends Reducer<Text, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String keyStr = key.toString();

        if (keyStr.equals("__TOTAL_WORDS__") || keyStr.equals("__TOTAL_WORDS_RAW__")) {
            long total = 0;
            for (Text val : values) {
                total += Long.parseLong(val.toString().trim());
            }
            outKey.set(keyStr);
            outValue.set(String.valueOf(total));
            context.write(outKey, outValue);
        } else if (keyStr.equals("__LONGEST_DESC__") || keyStr.equals("__LONGEST_DESC_RAW__")) {
            int maxLen = -1;
            String bestTitle = "";
            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                if (parts.length == 2) {
                    try {
                        int len = Integer.parseInt(parts[0].trim());
                        if (len > maxLen) {
                            maxLen = len;
                            bestTitle = parts[1];
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
            outKey.set(keyStr);
            outValue.set(maxLen + "\t" + bestTitle);
            context.write(outKey, outValue);
        } else if (keyStr.equals("__SHORTEST_DESC__") || keyStr.equals("__SHORTEST_DESC_RAW__")) {
            int minLen = Integer.MAX_VALUE;
            String bestTitle = "";
            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                if (parts.length == 2) {
                    try {
                        int len = Integer.parseInt(parts[0].trim());
                        if (len < minLen) {
                            minLen = len;
                            bestTitle = parts[1];
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
            outKey.set(keyStr);
            outValue.set(minLen + "\t" + bestTitle);
            context.write(outKey, outValue);
        } else if (keyStr.startsWith("WORD\t")) {
            long sum = 0;
            for (Text val : values) {
                sum += Long.parseLong(val.toString().trim());
            }
            outKey.set(keyStr);
            outValue.set(String.valueOf(sum));
            context.write(outKey, outValue);
        }
    }
}
