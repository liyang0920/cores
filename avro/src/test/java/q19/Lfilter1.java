package q19;

import cores.avro.FilterOperator;

public class Lfilter1 implements FilterOperator<Float> {
    float f1, f2;

    public Lfilter1(float f1, float f2) {
        this.f1 = f1;
        this.f2 = f2;
    }

    public String getName() {
        return "l_quantity";
    }

    public boolean isMatch(Float s) {
        if (s >= f1 && s <= f2) {
            return true;
        } else
            return false;
    }
}
