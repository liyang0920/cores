package pubQ1;

import cores.avro.FilterOperator;

public class VolumeFilter implements FilterOperator<String> {
    int v1, v2;

    public VolumeFilter(int s1, int s2) {
        v1 = s1;
        v2 = s2;
    }

    @Override
    public String getName() {
        return "Volume";
    }

    @Override
    public boolean isMatch(String s) {
        StringBuilder ins = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                ins.append(s.charAt(i++));
                while (i < s.length() && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                    ins.append(s.charAt(i++));
                }
                break;
            }
        }
        if (ins.length() == 0)
            return false;
        int in = Integer.parseInt(ins.toString());
        if (in >= v1 && in <= v2)
            return true;
        else
            return false;
    }

}
