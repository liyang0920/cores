package pubQ4;

import cores.avro.FilterOperator;

public class OwnerFilter implements FilterOperator<String> {
    String[] owner;

    public OwnerFilter(String[] s) {
        owner = s;
    }

    @Override
    public String getName() {
        return "Owner";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < owner.length; i++) {
            if (s.equals(owner[i]))
                return true;
        }
        return false;
    }
}
