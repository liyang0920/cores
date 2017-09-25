package q6;

import cores.avro.FilterOperator;

public class Lfilter3 implements FilterOperator<Float> {
    float f;

    public Lfilter3(float f) {
        this.f = f;
    }

    public String getName() {
        return "l_quantity";
    }

    public boolean isMatch(Float s) {
        if (s < f) {
            return true;
        } else
            return false;
    }
}
