package q14;

import cores.avro.FilterOperator;

public class Lfilter implements FilterOperator<String> {
    String t1, t2;

    public Lfilter(String s1, String s2) {
        t1 = s1;
        t2 = s2;
    }

    public String getName() {
        return "l_shipdate";
    }

    public boolean isMatch(String s) {
        if (s.compareTo(t1) >= 0 && s.compareTo(t2) < 0) {
            return true;
        } else
            return false;
    }

}
