/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cores.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.trevni.TrevniRuntimeException;

/**
 * Metadata for a column.
 */
public class FileColumnMetaData extends MetaData<FileColumnMetaData> {
    static final String NAME_KEY = RESERVED_KEY_PREFIX + "name";
    static final String TYPE_KEY = RESERVED_KEY_PREFIX + "type";
    static final String VALUES_KEY = RESERVED_KEY_PREFIX + "values";
    static final String PARENT_KEY = RESERVED_KEY_PREFIX + "parent";
    static final String ARRAY_KEY = RESERVED_KEY_PREFIX + "array";
    static final String LAYER_KEY = RESERVED_KEY_PREFIX + "layer";

    // cache these values for better performance
    private String name;
    private ValueType type;
    private boolean values;
    private FileColumnMetaData parent;
    private boolean isArray;
    private int layer;

    private transient List<FileColumnMetaData> children = new ArrayList<FileColumnMetaData>();
    private transient int number = -1;

    private FileColumnMetaData() {
    } // non-public ctor

    /**
     * Construct given a name and type.
     */
    public FileColumnMetaData(String name, ValueType type) {
        this.name = name;
        setReserved(NAME_KEY, name);
        this.type = type;
        setReserved(TYPE_KEY, type.getName());
        this.layer = 0;
        setReserved(LAYER_KEY, new String() + layer);
    }

    /**
     * Return this column's name.
     */
    public String getName() {
        return name;
    }

    /**
     * Return this column's type.
     */
    public ValueType getType() {
        return type;
    }

    /**
     * Return this column's parent or null.
     */
    public FileColumnMetaData getParent() {
        return parent;
    }

    /**
     * Return this column's children or null.
     */
    public List<FileColumnMetaData> getChildren() {
        return children;
    }

    /**
     * Return true if this column is an array.
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Return this column's number in a file.
     */
    public int getNumber() {
        return number;
    }

    void setNumber(int number) {
        this.number = number;
    }

    public int getLayer() {
        return layer;
    }

    public FileColumnMetaData setLayer(int layer) {
        this.layer = layer;
        return setReserved(LAYER_KEY, new String() + layer);
    }

    /**
     * Set whether this column has an index of blocks by value. This only makes
     * sense for sorted columns and permits one to seek into a column by value.
     */
    public FileColumnMetaData hasIndexValues(boolean values) {
        if (isArray)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.values = values;
        return setReservedBoolean(VALUES_KEY, values);
    }

    /**
     * Set this column's parent. A parent must be a preceding array column.
     */
    public FileColumnMetaData setParent(FileColumnMetaData parent) {
        if (!parent.isArray())
            throw new TrevniRuntimeException("Parent is not an array: " + parent);
        if (values)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.parent = parent;
        parent.children.add(this);
        return setReserved(PARENT_KEY, parent.getName());
    }

    /**
     * Set whether this column is an array.
     */
    public FileColumnMetaData isArray(boolean isArray) {
        if (values)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.isArray = isArray;
        return setReservedBoolean(ARRAY_KEY, isArray);
    }

    /**
     * Get whether this column has an index of blocks by value.
     */
    public boolean hasIndexValues() {
        return getBoolean(VALUES_KEY);
    }

    static FileColumnMetaData read(InputBuffer in, InsertColumnFileReader file) throws IOException {
        FileColumnMetaData result = new FileColumnMetaData();
        MetaData.read(in, result);
        result.name = result.getString(NAME_KEY);
        result.type = ValueType.forName(result.getString(TYPE_KEY));
        result.values = result.getBoolean(VALUES_KEY);
        result.isArray = result.getBoolean(ARRAY_KEY);
        result.layer = result.getInteger(LAYER_KEY);

        String parentName = result.getString(PARENT_KEY);
        if (parentName != null)
            result.setParent(file.getFileColumnMetaData(parentName));

        return result;
    }

    static FileColumnMetaData read(InputBuffer in, BatchColumnFileReader file) throws IOException {
        FileColumnMetaData result = new FileColumnMetaData();
        MetaData.read(in, result);
        result.name = result.getString(NAME_KEY);
        result.type = ValueType.forName(result.getString(TYPE_KEY));
        result.values = result.getBoolean(VALUES_KEY);
        result.isArray = result.getBoolean(ARRAY_KEY);
        result.layer = result.getInteger(LAYER_KEY);

        String parentName = result.getString(PARENT_KEY);
        if (parentName != null)
            result.setParent(file.getFileColumnMetaData(parentName));

        return result;
    }

    public void write(OutputBuffer out) throws IOException {
        super.write(out);
    }

}
