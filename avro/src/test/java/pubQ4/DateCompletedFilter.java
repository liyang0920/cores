package pubQ4;

import cores.avro.FilterOperator;

public class DateCompletedFilter implements FilterOperator<String> {
    String t1, t2;

    public DateCompletedFilter(String s1, String s2) {
        t1 = s1;
        t2 = s2;
    }

    @Override
    public String getName() {
        return "DateCompleted";
    }

    @Override
    public boolean isMatch(String s) {
        if (s.compareTo(t1) >= 0 && s.compareTo(t2) <= 0)
            return true;
        else
            return false;
    }
}
