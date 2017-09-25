package ppsl;

import cores.avro.FilterOperator;

public class lFilter implements FilterOperator<String> {
    public String getName() {
        return "l_shipdate";
    }

    public boolean isMatch(String s) {
        if (s.compareTo("1993-01-10") >= 0 && s.compareTo("1994-01-10") < 0) {
            return true;
        } else
            return false;
    }
}
