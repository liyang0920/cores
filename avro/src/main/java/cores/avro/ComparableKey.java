package cores.avro;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class ComparableKey implements Comparable<ComparableKey> {
    private Record record;
    private int[] keyFields;

    public ComparableKey(Record record, int[] keyFields) {
        this.record = record;
        this.keyFields = keyFields;
    }

    public Record getRecord() {
        return record;
    }

    public int[] getKeyFields() {
        return keyFields;
    }

    public int compareTo(ComparableKey o) {
        assert (keyFields.length == o.keyFields.length);
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < keyFields.length; i++) {
            if (isInteger(fs.get(keyFields[i]))) {
                long k1 = Long.parseLong(record.get(keyFields[i]).toString());
                long k2 = Long.parseLong(o.record.get(o.keyFields[i]).toString());
                if (k1 > k2) {
                    return 1;
                } else if (k1 < k2) {
                    return -1;
                }
            } else {
                String k1 = record.get(keyFields[i]).toString();
                String k2 = o.record.get(o.keyFields[i]).toString();
                if (k1.compareTo(k2) != 0) {
                    return k1.compareTo(k2);
                }
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((ComparableKey) o) == 0);
    }

    boolean isInteger(Field f) {
        switch (f.schema().getType()) {
            case LONG:
            case INT:
                return true;
            case STRING:
            case BYTES:
                return false;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + f.schema());
        }
    }
}
