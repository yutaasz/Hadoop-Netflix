package com.hadoop.netflix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class NetflixSummaryReducer extends Reducer<Text, Text, Text, Text> {
    private static class WordCount {
        String word;
        long count;

        WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
    }

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalWords = 0;
        long totalWordsRaw = 0;
        String longestDesc = "N/A";
        String shortestDesc = "N/A";
        String longestDescRaw = "N/A";
        String shortestDescRaw = "N/A";

        List<WordCount> wordList = new ArrayList<>();

        for (Text val : values) {
            String line = val.toString().trim();
            if (line.startsWith("__TOTAL_WORDS__\t")) {
                totalWords = Long.parseLong(line.substring("__TOTAL_WORDS__\t".length()).trim());
            } else if (line.startsWith("__TOTAL_WORDS_RAW__\t")) {
                totalWordsRaw = Long.parseLong(line.substring("__TOTAL_WORDS_RAW__\t".length()).trim());
            } else if (line.startsWith("__LONGEST_DESC__\t")) {
                longestDesc = line.substring("__LONGEST_DESC__\t".length()).trim();
            } else if (line.startsWith("__SHORTEST_DESC__\t")) {
                shortestDesc = line.substring("__SHORTEST_DESC__\t".length()).trim();
            } else if (line.startsWith("__LONGEST_DESC_RAW__\t")) {
                longestDescRaw = line.substring("__LONGEST_DESC_RAW__\t".length()).trim();
            } else if (line.startsWith("__SHORTEST_DESC_RAW__\t")) {
                shortestDescRaw = line.substring("__SHORTEST_DESC_RAW__\t".length()).trim();
            } else if (line.startsWith("WORD\t")) {
                String[] parts = line.split("\t");
                if (parts.length >= 3) {
                    String word = parts[1];
                    long count = Long.parseLong(parts[2].trim());
                    wordList.add(new WordCount(word, count));
                }
            }
        }

        // 1. Top 5 palavras mais repetidas (ordenadas por frequencia decrescente)
        Collections.sort(wordList, new Comparator<WordCount>() {
            @Override
            public int compare(WordCount a, WordCount b) {
                if (b.count != a.count) {
                    return Long.compare(b.count, a.count);
                }
                return a.word.compareTo(b.word);
            }
        });

        List<WordCount> top5 = new ArrayList<>();
        for (int i = 0; i < Math.min(5, wordList.size()); i++) {
            top5.add(wordList.get(i));
        }

        // 2. Palavras menos repetidas (ordenadas por frequencia crescente e ordem alfabetica)
        Collections.sort(wordList, new Comparator<WordCount>() {
            @Override
            public int compare(WordCount a, WordCount b) {
                if (a.count != b.count) {
                    return Long.compare(a.count, b.count);
                }
                return a.word.compareTo(b.word);
            }
        });

        // Filtrar palavras puramente alfabeticas para exibicao textual
        List<WordCount> bottom5Alpha = new ArrayList<>();
        for (WordCount wc : wordList) {
            if (wc.word.matches("^[a-z]+$")) {
                bottom5Alpha.add(wc);
                if (bottom5Alpha.size() == 5) break;
            }
        }

        // Primeiros 5 gerais (incluindo tokens com digitos)
        List<WordCount> bottom5All = new ArrayList<>();
        for (int i = 0; i < Math.min(5, wordList.size()); i++) {
            bottom5All.add(wordList.get(i));
        }

        // Construcao do relatorio final
        StringBuilder sb = new StringBuilder();
        sb.append("\n================================================================================\n");
        sb.append("                      TRABALHO PRATICO - MAPREDUCE NO HADOOP\n");
        sb.append("                       ANALISE DO DATASET NETFLIX TITLES\n");
        sb.append("================================================================================\n\n");

        sb.append("1. NORMALIZACAO DE TEXTO (0,5 pts) e STOPWORDS (1,0 pt):\n");
        sb.append("   - Minusculas: Aplicado (toLowerCase)\n");
        sb.append("   - Remocao de pontuacao e caracteres especiais: Aplicado (regex [^a-z0-9\\s])\n");
        sb.append("   - Stopwords ignoradas: Sim (stopwords da lingua inglesa)\n\n");

        sb.append("2. TITULO COM DESCRICAO MAIS LONGA (0,5 pt):\n");
        String[] longestParts = longestDesc.split("\t", 2);
        if (longestParts.length == 2) {
            sb.append("   - Sem stopwords: \"").append(longestParts[1]).append("\" (").append(longestParts[0]).append(" palavras)\n");
        } else {
            sb.append("   - Sem stopwords: ").append(longestDesc).append("\n");
        }
        String[] longestRawParts = longestDescRaw.split("\t", 2);
        if (longestRawParts.length == 2) {
            sb.append("   - Total bruto (com stopwords): \"").append(longestRawParts[1]).append("\" (").append(longestRawParts[0]).append(" palavras)\n\n");
        } else {
            sb.append("   - Total bruto (com stopwords): ").append(longestDescRaw).append("\n\n");
        }

        sb.append("3. TITULO COM DESCRICAO MAIS CURTA (0,5 pt):\n");
        String[] shortestParts = shortestDesc.split("\t", 2);
        if (shortestParts.length == 2) {
            sb.append("   - Sem stopwords: \"").append(shortestParts[1]).append("\" (").append(shortestParts[0]).append(" palavras)\n");
        } else {
            sb.append("   - Sem stopwords: ").append(shortestDesc).append("\n");
        }
        String[] shortestRawParts = shortestDescRaw.split("\t", 2);
        if (shortestRawParts.length == 2) {
            sb.append("   - Total bruto (com stopwords): \"").append(shortestRawParts[1]).append("\" (").append(shortestRawParts[0]).append(" palavras)\n\n");
        } else {
            sb.append("   - Total bruto (com stopwords): ").append(shortestDescRaw).append("\n\n");
        }

        sb.append("4. TOP 5 PALAVRAS MAIS REPETIDAS NAS DESCRICOES (1,0 pt):\n");
        for (int i = 0; i < top5.size(); i++) {
            WordCount wc = top5.get(i);
            sb.append(String.format("   %d) '%s' - %d ocorrencias\n", (i + 1), wc.word, wc.count));
        }
        sb.append("\n");

        sb.append("5. TOP 5 PALAVRAS MENOS REPETIDAS NAS DESCRICOES (1,0 pt):\n");
        sb.append("   [Palavras textuais / alfabeticas - todas com 1 ocorrencia]:\n");
        for (int i = 0; i < bottom5Alpha.size(); i++) {
            WordCount wc = bottom5Alpha.get(i);
            sb.append(String.format("   %d) '%s' - %d ocorrencia\n", (i + 1), wc.word, wc.count));
        }
        sb.append("   [Tokens alfanumericos gerais - todos com 1 ocorrencia]:\n");
        for (int i = 0; i < bottom5All.size(); i++) {
            WordCount wc = bottom5All.get(i);
            sb.append(String.format("   %d) '%s' - %d ocorrencia\n", (i + 1), wc.word, wc.count));
        }
        sb.append("\n");

        sb.append("6. NUMERO TOTAL DE PALAVRAS EM TODAS AS DESCRICOES (0,5 pts):\n");
        sb.append("   - Total de palavras (ignorando stopwords): ").append(totalWords).append(" palavras\n");
        sb.append("   - Total de palavras brutas (incluindo stopwords): ").append(totalWordsRaw).append(" palavras\n");
        sb.append("================================================================================\n");

        outKey.set("RELATORIO_FINAL");
        outValue.set(sb.toString());
        context.write(outKey, outValue);
    }
}