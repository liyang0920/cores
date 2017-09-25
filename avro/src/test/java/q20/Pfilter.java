package q20;

import cores.avro.FilterOperator;

public class Pfilter implements FilterOperator<String> {
    String[] start;

    public Pfilter(String[] s) {
        start = s;
    }

    public String getName() {
        return "p_name";
    }

    public boolean isMatch(String s) {
        for (int i = 0; i < start.length; i++) {
            if (s.contains(start[i]))
                return true;
        }
        return false;
    }
}
