package q19;

import cores.avro.FilterOperator;

public class Pfilter3 implements FilterOperator<Integer> {
    int com;

    public Pfilter3(int s) {
        com = s;
    }

    public String getName() {
        return "p_size";
    }

    public boolean isMatch(Integer s) {
        if (s >= 1 && s <= com) {
            return true;
        } else
            return false;
    }
}
