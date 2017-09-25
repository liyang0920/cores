package ppsl;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import cores.avro.FilterBatchColumnReader;
import cores.avro.FilterOperator;

public class FilterRead {
    public static void main(String[] args) throws IOException {
        FilterOperator[] filters = new FilterOperator[2];
        filters[0] = new pFilter();
        filters[1] = new lFilter();
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        reader.filter();
        reader.createFilterRead(1000);
        int count = 0;
        while (reader.hasNext()) {
            Record r = reader.next();
            count++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
    }
}
