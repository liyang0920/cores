package pubQ3;

import cores.avro.FilterOperator;

public class DateCreatedFilter implements FilterOperator<String> {
    String t1, t2;

    public DateCreatedFilter(String s1, String s2) {
        t1 = s1;
        t2 = s2;
    }

    @Override
    public String getName() {
        return "DateCreated";
    }

    @Override
    public boolean isMatch(String s) {
        if (s.compareTo(t1) >= 0 && s.compareTo(t2) <= 0)
            return true;
        else
            return false;
    }
}
