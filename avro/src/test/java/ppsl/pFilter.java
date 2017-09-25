package ppsl;

import cores.avro.FilterOperator;

public class pFilter implements FilterOperator<String> {
    public String getName() {
        return "p_name";
    }

    public boolean isMatch(String s) {
        if (s.contains("green")) {
            return true;
        } else
            return false;
    }
}
