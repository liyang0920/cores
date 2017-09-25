package q19;

import cores.avro.FilterOperator;

public class Pfilter2 implements FilterOperator<String> {
    String[] com;

    public Pfilter2(String[] s) {
        com = s;
    }

    public String getName() {
        return "p_container";
    }

    public boolean isMatch(String s) {
        for (int i = 0; i < com.length; i++) {
            if (s.startsWith(com[i]))
                return true;
        }
        return false;
    }
}
