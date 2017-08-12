package cores.avro;

import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class MyComparator implements Comparator<Record> {
    @Override
    public int compare(Record o1, Record o2) {
        List<Field> ff = o1.getSchema().getFields();
        for (int i = 0; i < ff.size(); i++) {
            int m = compare(o1.get(i), o2.get(i), ff.get(i));
            if (m != 0) {
                return m;
            }
        }
        return 0;
    }

    public int compare(Object o1, Object o2, Field f) {
        switch (f.schema().getType()) {
            case INT:
                int i1 = Integer.parseInt(o1.toString());
                int i2 = Integer.parseInt(o2.toString());
                return (i1 > i2) ? 1 : ((i1 == i2) ? 0 : -1);
            case LONG:
                long l1 = Long.parseLong(o1.toString());
                long l2 = Long.parseLong(o2.toString());
                return (l1 > l2) ? 1 : ((l1 == l2) ? 0 : -1);
            case FLOAT:
                float f1 = Float.parseFloat(o1.toString());
                float f2 = Float.parseFloat(o2.toString());
                return (f1 > f2) ? 1 : ((f1 == f2) ? 0 : -1);
            case DOUBLE:
                double d1 = Double.parseDouble(o1.toString());
                double d2 = Double.parseDouble(o2.toString());
                return (d1 > d2) ? 1 : ((d1 == d2) ? 0 : -1);
            case BYTES:
            case STRING:
                return o1.toString().compareTo(o2.toString());
            case ARRAY:
                return 0;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + f.schema());
        }
    }
}
