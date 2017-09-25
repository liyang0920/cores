package q19;

import cores.avro.FilterOperator;

public class Pfilter implements FilterOperator<Integer> {
    int k;

    public Pfilter(int k) {
        this.k = k;
    }

    public String getName() {
        return "p_partkey";
    }

    public boolean isMatch(Integer s) {
        if (s <= k) {
            return true;
        } else
            return false;
    }
}
