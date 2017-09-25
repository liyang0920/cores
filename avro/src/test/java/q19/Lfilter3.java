package q19;

import cores.avro.FilterOperator;

public class Lfilter3 implements FilterOperator<String> {
    public String getName() {
        return "l_shipinstruct";
    }

    public boolean isMatch(String s) {
        if (s.equals("DELIVER IN PERSON")) {
            return true;
        } else
            return false;
    }
}
