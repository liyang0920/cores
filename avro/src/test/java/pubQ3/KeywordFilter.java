package pubQ3;

import cores.avro.FilterOperator;

public class KeywordFilter implements FilterOperator<String> {
    String[] word;

    public KeywordFilter(String[] s) {
        word = s;
    }

    @Override
    public String getName() {
        return "keyword";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < word.length; i++) {
            if (s.contains(word[i]))
                return true;
        }
        return false;
    }
}
