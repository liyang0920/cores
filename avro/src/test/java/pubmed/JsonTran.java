package pubmed;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import cores.avro.BatchAvroColumnWriter;

public class JsonTran {
    public static void main(String[] args) throws Exception {
        //        OutputStream outs = Util.fileOrStdout(args.get(1), out);
        Schema s = new Schema.Parser().parse(new File(args[1]));

        DatumReader<Record> reader = new GenericDatumReader<Record>(s);
        InputStream input = Util.openFromFS(args[0]);

        try {
            DataInputStream din = new DataInputStream(input);
            Decoder decoder = DecoderFactory.get().jsonDecoder(s, din);
            BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, args[2],
                    Integer.parseInt(args[3]), Integer.parseInt(args[4]));
            Record datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException e) {
                    break;
                }
                writer.append(datum);
            }
            int index = writer.flush();
            File[] files = new File[index];
            for (int i = 0; i < index; i++)
                files[i] = new File(args[2] + "file" + String.valueOf(i) + ".neci");
            if (index == 1) {
                new File(args[2] + "file0.head").renameTo(new File(args[2] + "result.head"));
                new File(args[2] + "file0.neci").renameTo(new File(args[2] + "result.neci"));
            } else {
                writer.mergeFiles(files);
            }
        } finally {
            Util.close(input);
        }
    }
}
