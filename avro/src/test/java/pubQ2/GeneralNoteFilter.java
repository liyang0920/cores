package pubQ2;

import cores.avro.FilterOperator;

public class GeneralNoteFilter implements FilterOperator<String> {
    String[] note;

    public GeneralNoteFilter(String[] s) {
        note = s;
    }

    @Override
    public String getName() {
        return "generalNote";
    }

    @Override
    public boolean isMatch(String s) {
        for (int i = 0; i < note.length; i++) {
            if (s.contains(note[i]))
                return true;
        }
        return false;
    }
}
