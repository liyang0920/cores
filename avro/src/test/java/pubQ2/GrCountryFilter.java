package pubQ2;

import cores.avro.FilterOperator;

public class GrCountryFilter implements FilterOperator<String> {
    String[] country;

    public GrCountryFilter(String[] s) {
        country = s;
    }

    @Override
    public String getName() {
        return "GrCountry";
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
