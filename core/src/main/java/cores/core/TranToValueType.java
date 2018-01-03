package cores.core;

import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;

public class TranToValueType {
    public static ValueType tran(Object datum) {
        if (datum == null)
            return ValueType.NULL;
        if (datum instanceof Boolean)
            return ValueType.BOOLEAN;
        if (datum instanceof Integer)
            return ValueType.INT;
        if (datum instanceof Long)
            return ValueType.LONG;
        if (datum instanceof Float)
            return ValueType.FLOAT;
        if (datum instanceof Double)
            return ValueType.DOUBLE;
        if (datum instanceof ByteBuffer)
            return ValueType.BYTES;
        if (datum instanceof Byte[])
            return ValueType.BYTES;
        if (datum instanceof CharSequence)
            return ValueType.STRING;
        else
            throw new TrevniRuntimeException("Illegal value logical type: " + datum.getClass());
    }
}
