package pubQ1;

import cores.avro.FilterOperator;

public class MJCountryFilter implements FilterOperator<String> {
    String[] country;

    public MJCountryFilter(String[] s) {
        country = s;
    }

    @Override
    public String getName() {
        return "MJCountry";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < country.length; i++) {
            if (s.equals(country[i]))
                return true;
        }
        return false;
    }
}
