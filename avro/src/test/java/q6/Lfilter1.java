package q6;

import cores.avro.FilterOperator;

public class Lfilter1 implements FilterOperator<String> {
    String t1, t2;

    public Lfilter1(String t1, String t2) {
        this.t1 = t1;
        this.t2 = t2;
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
