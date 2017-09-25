package pubmed;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import cores.avro.BatchAvroColumnWriter;

public class AvroTran {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema s = new Schema.Parser().parse(new File(args[1]));
        DatumReader<Record> reader = new GenericDatumReader<Record>(s);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, args[2], Integer.parseInt(args[3]),
                Integer.parseInt(args[4]));

        while (fileReader.hasNext()) {
            writer.append(fileReader.next());
        }
        fileReader.close();
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(args[2] + "file" + String.valueOf(i) + ".trv");
        if (index == 1) {
            new File(args[2] + "file0.head").renameTo(new File(args[2] + "result.head"));
            new File(args[2] + "file0.trv").renameTo(new File(args[2] + "result.trv"));
        } else {
            writer.mergeFiles(files);
        }
    }
}
