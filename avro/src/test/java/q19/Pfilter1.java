package q19;

import cores.avro.FilterOperator;

public class Pfilter1 implements FilterOperator<String> {
    String com;

    public Pfilter1(String s) {
        com = s;
    }

    public String getName() {
        return "p_brand";
    }

    public boolean isMatch(String s) {
        if (s.compareTo(com) <= 0) {
            return true;
        } else
            return false;
    }
}
