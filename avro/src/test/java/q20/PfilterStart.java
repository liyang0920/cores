package q20;

import cores.avro.FilterOperator;

public class PfilterStart implements FilterOperator<String> {
    String start;

    public PfilterStart(String s) {
        start = s;
    }

    public String getName() {
        return "p_name";
    }

    public boolean isMatch(String s) {
        if (s.startsWith(start)) {
            return true;
        } else
            return false;
    }
}
