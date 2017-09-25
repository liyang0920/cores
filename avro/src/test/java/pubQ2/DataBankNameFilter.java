package pubQ2;

import cores.avro.FilterOperator;

public class DataBankNameFilter implements FilterOperator<String> {
    String[] name;

    public DataBankNameFilter(String[] s) {
        name = s;
    }

    @Override
    public String getName() {
        return "DataBankName";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < name.length; i++) {
            if (s.equals(name[i]))
                return true;
        }
        return false;
    }
}
