package q6;

import cores.avro.FilterOperator;

public class Lfilter2 implements FilterOperator<Float> {
    float f1, f2;

    public Lfilter2(float t1, float t2) {
        f1 = t1;
        f2 = t2;
    }

    public String getName() {
        return "l_discount";
    }

    public boolean isMatch(Float s) {
        if (s >= f1 && s <= f2) {
            return true;
        } else
            return false;
    }
}
