package cores.avro;

import static cores.avro.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.TrevniRuntimeException;

import cores.core.ColumnValues;
import cores.core.FileColumnMetaData;
import cores.core.InsertColumnFileReader;

public class InsertAvroColumnReader<D> implements Iterator<D>, Iterable<D>, Closeable {
    private InsertColumnFileReader reader;
    private Schema fileSchema;
    private Schema readSchema;
    private GenericData model;

    //private List<Integer> columns;
    private int column;
    private ColumnValues[] values;
    private int[] arrayWidths;

    //private Map<String,Map<String,Object>> defaults =
    //new HashMap<String,Map<String,Object>>();

    public static class Params {
        File file;
        Schema schema;
        GenericData model = GenericData.get();

        public Params(File file) {
            this.file = file;
        }

        public Params setSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Params setModel(GenericData model) {
            this.model = model;
            return this;
        }
    }

    public InsertAvroColumnReader(Params params) throws IOException {
        this.reader = new InsertColumnFileReader(params.file);
        this.model = params.model;
        this.fileSchema = Schema.parse(reader.getMetaData().getString(InsertAvroColumnWriter.SCHEMA_KEY));
        this.readSchema = params.schema == null ? fileSchema : params.schema;
        initialize();
    }

    void initialize() throws IOException {
        //建立文件的列名称和列号之间的map
        Map<String, Integer> fileColumnNumbers = new HashMap<String, Integer>();
        int i = 0;
        for (FileColumnMetaData c : new AvroColumnator(fileSchema).getColumns()) {
            fileColumnNumbers.put(c.getName(), i++);
        }

        AvroColumnator readColumnator = new AvroColumnator(readSchema);
        this.arrayWidths = readColumnator.getArrayWidths();
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        this.values = new ColumnValues[readColumns.length];
        int j = 0;
        for (FileColumnMetaData c : readColumns) {
            Integer n = fileColumnNumbers.get(c.getName());
            if (n != null) {
                //columns.add(n);
                values[j++] = reader.getValues(n);
            } else {
                throw new TrevniRuntimeException("Unknown column: " + c);
            }
        }
        //findDefaults(readSchema, fileSchema, true);
    }

    //对每个在读schema中而不在写schema中的fields设置default值，并检查匹配性
    //  private void findDefaults(Schema read, Schema write, boolean m){
    //    switch(read.getType()){
    //      case NULL: case BOOLEAN:
    //      case INT: case LONG:
    //      case FLOAT: case DOUBLE:
    //      case BYTES: case STRING:
    //      case ENUM: case FIXED:
    //        if(read.getType() != write.getType())
    //          throw new TrevniRuntimeException("Type mismatch: "+read+"&"+write);
    //        break;
    //      case MAP:
    //        findDefaults(read.getValueType(), write.getValueType(), m);
    //        break;
    //      case ARRAY:
    //        findDefaults(read.getElementType(), write.getElementType(), m);
    //        break;
    //      case UNION:
    //        for (Schema s : read.getTypes()) {
    //          Integer index = write.getIndexNamed(s.getFullName());
    //          if (index == null)
    //            throw new TrevniRuntimeException("No matching branch: "+s);
    //          findDefaults(s, write.getTypes().get(index), m);
    //        }
    //        break;
    //      case RECORD:
    //        boolean x = true;
    //        for (Field f : read.getFields()) {
    //          Field g = write.getField(f.name());
    //          if (g == null)
    //            setDefault(read, f);
    //          else{
    //            x = false;
    //            findDefaults(f.schema(), g.schema(), x);
    //          }
    //        }
    //        if(x && m){
    //          for(Field f : write.getFields()){
    //            switch(f.schema().getType()){
    //              case ARRAY:
    //                findDefaults(read,f.schema().getElementType(), x);
    //                break;
    //              default: break;
    //            }
    //          }
    //        }
    //        break;
    //      default:
    //        throw new TrevniRuntimeException("Unknown schema: "+read);
    //    }
    //  }

    //  private void setDefault(Schema record, Field f) {
    //    String recordName = record.getFullName();
    //    Map<String,Object> recordDefaults = defaults.get(recordName);
    //    if (recordDefaults == null) {
    //      recordDefaults = new HashMap<String,Object>();
    //      defaults.put(recordName, recordDefaults);
    //    }
    //    recordDefaults.put(f.name(), model.getDefaultValue(f));
    //  }

    @Override
    public Iterator<D> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return values[0].hasNext();
    }

    public int getRowCount() {
        return reader.getRowCount();
    }

    @Override
    public D next() {
        try {
            this.column = 0;
            return (D) read(readSchema);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    private Object read(Schema s) throws IOException {
        if (isSimple(s))
            return nextValue(s, column++);
        final int startColumn = column;

        switch (s.getType()) {
            case MAP:
                values[column].startRow();
                int size = values[column].nextLength();
                Map map = (Map) new HashMap(size);
                for (int i = 0; i < size; i++) {
                    this.column = startColumn;
                    values[column].startRow();
                    values[column++].nextValue();
                    values[column].startRow();
                    String key = (String) values[column++].nextValue();
                    map.put(key, read(s.getValueType()));
                }
                column = startColumn + arrayWidths[startColumn];
                return map;
            case RECORD:
                Object record = model.newRecord(null, s);
                for (Field f : s.getFields()) {
                    Object value = read(f.schema());
                    model.setField(record, f.name(), f.pos(), value);
                }
                return record;
            case ARRAY:
                values[column].startRow();
                int length = values[column].nextLength();
                List elements = (List) new GenericData.Array(length, s);
                for (int i = 0; i < length; i++) {
                    this.column = startColumn;
                    Object value;
                    if (isSimple(s.getElementType()))
                        value = nextValue(s, ++column);
                    else {
                        column++;
                        value = read(s.getElementType());
                    }
                    elements.add(value);
                }
                column = startColumn + arrayWidths[startColumn];
                return elements;
            case UNION:
                Object value = null;
                for (Schema branch : s.getTypes()) {
                    if (branch.getType() == Schema.Type.NULL)
                        continue;
                    values[column].startRow();
                    if (values[column].nextLength() == 1) {
                        value = nextValue(branch, column);
                        column++;
                        if (!isSimple(branch))
                            value = read(branch);
                    } else {
                        column += arrayWidths[column];
                    }
                }
                return value;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    private Object nextValue(Schema s, int column) throws IOException {
        values[column].startRow();
        Object v = values[column].nextValue();

        switch (s.getType()) {
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
        }

        return v;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
