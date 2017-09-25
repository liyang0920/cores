package q19;

import cores.avro.FilterOperator;

public class Lfilter2 implements FilterOperator<String> {
    String[] com;

    public Lfilter2(String[] s) {
        com = s;
    }

    public String getName() {
        return "l_shipmode";
    }

    public boolean isMatch(String s) {
        for (int i = 0; i < com.length; i++) {
            if (s.startsWith(com[i]))
                return true;
        }
        return false;
    }
}
