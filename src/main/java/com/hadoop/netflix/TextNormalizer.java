package com.hadoop.netflix;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TextNormalizer {
    private final Set<String> stopwords = new HashSet<>();

    public TextNormalizer() {
        loadStopwords();
    }

    private void loadStopwords() {
        // Fallback default English stopwords
        String[] defaultStopwords = {
            "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
            "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can",
            "can't", "cannot", "could", "couldn't", "d", "did", "didn't", "do", "does", "doesn't", "doing", "don't",
            "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have",
            "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him",
            "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't",
            "it", "it's", "its", "itself", "just", "let's", "ll", "m", "me", "more", "most", "mustn't", "my",
            "myself", "no", "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", "ought",
            "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan't", "she", "she'd", "she'll",
            "she's", "should", "shouldn't", "so", "some", "such", "t", "than", "that", "that's", "the", "their",
            "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll",
            "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very",
            "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when",
            "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "will", "with",
            "won't", "would", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
            "yourself", "yourselves"
        };
        stopwords.addAll(Arrays.asList(defaultStopwords));

        // Attempt loading from resource file
        try (InputStream in = getClass().getResourceAsStream("/stopwords.txt")) {
            if (in != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String word = line.trim().toLowerCase();
                        if (!word.isEmpty()) {
                            stopwords.add(word);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
    }

    public List<String> normalize(String text) {
        List<String> result = new ArrayList<>();
        if (text == null || text.trim().isEmpty()) {
            return result;
        }

        // 1. Minúsculas
        String lower = text.toLowerCase();

        // 2. Remoção de pontuações e caracteres especiais
        String cleaned = lower.replaceAll("[^a-z0-9\\s]", " ");

        // 3. Quebra em tokens
        String[] tokens = cleaned.split("\\s+");

        for (String token : tokens) {
            token = token.trim();
            // 4. Ignorar stopwords e tokens vazios
            if (!token.isEmpty() && !stopwords.contains(token)) {
                result.add(token);
            }
        }

        return result;
    }

    public boolean isStopword(String word) {
        return stopwords.contains(word.toLowerCase());
    }
}
