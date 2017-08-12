package cores.avro;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ValueType;

public class CombKey implements Comparable<CombKey> {
    private int[] keys;

    public CombKey(int len) {
        keys = new int[len];
    }

    public CombKey(Record record) {
        List<Field> fs = record.getSchema().getFields();
        int len = fs.size();
        this.keys = new int[len];
        for (int i = 0; i < len; i++) {
            keys[i] = Integer.parseInt(record.get(i).toString());
        }
    }

    public CombKey(String k) {
        this(k.split("\\|"));
    }

    public CombKey(String[] k) {
        keys = new int[k.length];
        for (int i = 0; i < k.length; i++)
            keys[i] = Integer.parseInt(k[i]);
    }

    public CombKey(Record record, int len) {
        this.keys = new int[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            keys[i] = Integer.parseInt(record.get(i).toString());
        }
    }

    public CombKey(Record record, int[] keyFields) {
        int len = keyFields.length;
        this.keys = new int[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            keys[i] = Integer.parseInt(record.get(keyFields[i]).toString());
        }
    }

    public CombKey(int[] keys) {
        this.keys = keys;
    }

    public static boolean isInteger(Field f) {
        switch (f.schema().getType()) {
            case INT:
                return true;
            case LONG:
                return false;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + f.schema());
        }
    }

    boolean isInteger(ValueType type) {
        switch (type) {
            case INT:
                return true;
            case LONG:
                return false;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + type);
        }
    }

    @Override
    public int compareTo(CombKey o) {
        assert (this.getLength() == o.getLength());
        for (int i = 0; i < getLength(); i++) {
            if (keys[i] > o.keys[i])
                return 1;
            else if (keys[i] < o.keys[i])
                return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        String res = "";
        for (int i = 0; i < keys.length - 1; i++)
            res += keys[i] + "|";
        res += keys[keys.length - 1];
        return res;
    }

    @Override
    public int hashCode() {
        return keys[0];
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((CombKey) o) == 0);
    }

    public int getLength() {
        return keys.length;
    }

    public int[] get() {
        return keys;
    }

    public Object get(int i) {
        return keys[i];
    }

    public CombKey get(int[] fields) {
        int[] k = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            k[i] = keys[fields[i]];
        }
        return new CombKey(k);
    }
}
