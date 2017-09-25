package pubQ3;

import cores.avro.FilterOperator;

public class AuLastNameFilter implements FilterOperator<String> {
    String[] name;

    public AuLastNameFilter(String[] s) {
        name = s;
    }

    @Override
    public String getName() {
        return "AuLastName";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < name.length; i++) {
            if (s.startsWith(name[i]))
                return true;
        }
        return false;
    }
}
