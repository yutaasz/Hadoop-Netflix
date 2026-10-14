package com.hadoop.netflix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class NetflixStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outValue = new Text();
    private TextNormalizer normalizer;

    // Track local min/max for filtered words
    private String maxTitleFiltered = null;
    private int maxCountFiltered = -1;
    private String minTitleFiltered = null;
    private int minCountFiltered = Integer.MAX_VALUE;

    // Track local min/max for raw words
    private String maxTitleRaw = null;
    private int maxCountRaw = -1;
    private String minTitleRaw = null;
    private int minCountRaw = Integer.MAX_VALUE;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        normalizer = new TextNormalizer();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("show_id,") || line.startsWith("show_id\t")) {
            return;
        }

        List<String> fields = CsvParser.parseLine(line);
        if (fields.size() < 12) {
            return;
        }

        String title = fields.get(2).replace("\"", "").trim();
        String description = fields.get(11).replace("\"", "").trim();

        // 1. Normalização sem stopwords (Tarefas 1 e 7)
        List<String> wordsFiltered = normalizer.normalize(description);
        int countFiltered = wordsFiltered.size();

        // 2. Normalização com todas as palavras (apenas minúsculas + sem pontuação)
        String lowerCleaned = description.toLowerCase().replaceAll("[^a-z0-9\\s]", " ").trim();
        String[] tokensRaw = lowerCleaned.isEmpty() ? new String[0] : lowerCleaned.split("\\s+");
        int countRaw = tokensRaw.length;

        if (countFiltered > 0) {
            if (countFiltered > maxCountFiltered) {
                maxCountFiltered = countFiltered;
                maxTitleFiltered = title;
            }
            if (countFiltered < minCountFiltered) {
                minCountFiltered = countFiltered;
                minTitleFiltered = title;
            }

            // Emit total words (ignoring stopwords)
            outKey.set("__TOTAL_WORDS__");
            outValue.set(String.valueOf(countFiltered));
            context.write(outKey, outValue);

            // Emit each word (ignoring stopwords)
            for (String word : wordsFiltered) {
                outKey.set("WORD\t" + word);
                outValue.set("1");
                context.write(outKey, outValue);
            }
        }

        if (countRaw > 0) {
            if (countRaw > maxCountRaw) {
                maxCountRaw = countRaw;
                maxTitleRaw = title;
            }
            if (countRaw < minCountRaw) {
                minCountRaw = countRaw;
                minTitleRaw = title;
            }

            // Emit total raw words
            outKey.set("__TOTAL_WORDS_RAW__");
            outValue.set(String.valueOf(countRaw));
            context.write(outKey, outValue);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (maxTitleFiltered != null) {
            outKey.set("__LONGEST_DESC__");
            outValue.set(maxCountFiltered + "\t" + maxTitleFiltered);
            context.write(outKey, outValue);
        }
        if (minTitleFiltered != null) {
            outKey.set("__SHORTEST_DESC__");
            outValue.set(minCountFiltered + "\t" + minTitleFiltered);
            context.write(outKey, outValue);
        }
        if (maxTitleRaw != null) {
            outKey.set("__LONGEST_DESC_RAW__");
            outValue.set(maxCountRaw + "\t" + maxTitleRaw);
            context.write(outKey, outValue);
        }
        if (minTitleRaw != null) {
            outKey.set("__SHORTEST_DESC_RAW__");
            outValue.set(minCountRaw + "\t" + minTitleRaw);
            context.write(outKey, outValue);
        }
    }
}
