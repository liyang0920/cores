/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cores.avro;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.trevni.TrevniRuntimeException;

import cores.core.FileColumnMetaData;
import cores.core.ValueType;

/** Utility that computes the column layout of a schema. */
class AvroColumnator {

    private Schema schema;

    private List<FileColumnMetaData> columns = new ArrayList<FileColumnMetaData>();
    private List<Integer> arrayWidths = new ArrayList<Integer>();

    public AvroColumnator(Schema schema) {
        this.schema = schema;
        columnize(null, schema, null, false);
    }

    /** Return columns for the schema. */
    public FileColumnMetaData[] getColumns() {
        return columns.toArray(new FileColumnMetaData[columns.size()]);
    }

    /**
     * Return array giving the number of columns immediately following each
     * column that are descendents of that column.
     */
    public int[] getArrayWidths() {
        int[] result = new int[arrayWidths.size()];
        int i = 0;
        for (Integer width : arrayWidths)
            result[i++] = width;
        return result;
    }

    private Map<Schema, Schema> seen = new IdentityHashMap<Schema, Schema>();

    private void columnize(String path, Schema s, FileColumnMetaData parent, boolean isArray) {
        if (isSimple(s)) {
            if (path == null)
                path = s.getFullName();
            addColumn(path, simpleValueType(s), parent, isArray);
            return;
        }

        if (seen.containsKey(s)) // catch recursion
            throw new TrevniRuntimeException("Cannot shred recursive schemas: " + s);
        seen.put(s, s);

        switch (s.getType()) {
            case MAP:
                path = path == null ? ">" : path + ">";
                int start = columns.size();
                FileColumnMetaData p = addColumn(path, ValueType.NULL, parent, true);
                addColumn(p(path, "key", ""), ValueType.STRING, p, false);
                columnize(p(path, "value", ""), s.getValueType(), p, false);
                arrayWidths.set(start, columns.size() - start); // fixup with actual width
                break;
            case RECORD:
                for (Field field : s.getFields()) // flatten fields to columns
                    columnize(field.name(), field.schema(), parent, isArray);
                break;
            case ARRAY:
                path = path == null ? "[]" : path + "[]";
                addArrayColumn(path, s.getElementType(), parent);
                break;
            case UNION:
                for (Schema branch : s.getTypes()) // array per non-null branch
                    if (branch.getType() != Schema.Type.NULL)
                        addArrayColumn(p(path, branch, "/"), branch, parent);
                break;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
        seen.remove(s);
    }

    private String p(String parent, Schema child, String sep) {
        if (child.getType() == Schema.Type.UNION)
            return parent;
        return p(parent, child.getFullName(), sep);
    }

    private String p(String parent, String child, String sep) {
        return parent == null ? child : parent + sep + child;
    }

    private FileColumnMetaData addColumn(String path, ValueType type, FileColumnMetaData parent, boolean isArray) {
        FileColumnMetaData column = new FileColumnMetaData(path, type);
        if (parent != null) {
            column.setParent(parent);
            column.setLayer(parent.getLayer() + 1);
        }
        column.isArray(isArray);
        columns.add(column);
        arrayWidths.add(1); // placeholder
        return column;
    }

    private void addArrayColumn(String path, Schema element, FileColumnMetaData parent) {
        if (path == null)
            path = element.getFullName();
        //        if (isSimple(element)) { // optimize simple arrays
        //            addColumn(path, simpleValueType(element), parent, true);
        //            return;
        //        }
        // complex array: insert a parent column with lengths
        int start = columns.size();
        FileColumnMetaData array = addColumn(path, ValueType.NULL, parent, true);
        if (isSimple(element)) {
            String ePath = path.substring(0, path.length() - 6);
            addColumn(ePath, simpleValueType(element), array, false);
        } else
            columnize(path, element, array, false);
        arrayWidths.set(start, columns.size() - start); // fixup with actual width
    }

    static boolean isSimple(Schema s) {
        switch (s.getType()) {
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case STRING:
            case ENUM:
            case FIXED:
                return true;
            default:
                return false;
        }
    }

    private ValueType simpleValueType(Schema s) {
        switch (s.getType()) {
            case NULL:
                return ValueType.NULL;
            case BOOLEAN:
                return ValueType.BOOLEAN;
            case INT:
                return ValueType.INT;
            case LONG:
                return ValueType.LONG;
            case FLOAT:
                return ValueType.FLOAT;
            case DOUBLE:
                return ValueType.DOUBLE;
            case BYTES:
                return ValueType.BYTES;
            case STRING:
                return ValueType.STRING;
            case ENUM:
                return ValueType.INT;
            case FIXED:
                return ValueType.BYTES;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

}
