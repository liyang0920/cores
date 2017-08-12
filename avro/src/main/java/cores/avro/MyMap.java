package cores.avro;

import java.util.Comparator;
import java.util.Map;

public class MyMap<K, V> {
    private Comparator<? super K> comparator;

    //private transient Entry<K, V> head = null;
    private transient Entry<K, V> tail = null;

    private transient int size = 0;
    private transient int modCount = 0;

    public MyMap() {
        comparator = null;
    }

    public MyMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
    }

    public V get(Object key) {
        Entry<K, V> p = getEntry(key);
        return (p == null ? null : p.getValue());
    }

    public Comparator<? super K> comparator() {
        return comparator;
    }

    public boolean containsKey(Object key) {
        return getEntry(key) != null;
    }

    public boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        for (Entry<K, V> p = tail; p != null; p = p.left) {
            if (value.equals(p.getValue()))
                return true;
        }
        return false;
    }

    public boolean contains(Entry<K, V> t) {
        if (t == null) {
            throw new NullPointerException();
        }
        for (Entry<K, V> p = tail; p != null; p = p.left) {
            if (t.getKey().equals(p.getKey()) && t.getValue().equals(p.getValue()))
                return true;
        }
        return false;
    }

    public boolean isEmpty() {
        return (tail == null);
    }

    public int size() {
        return size;
    }

    public void clear() {
        modCount++;
        size = 0;
        tail = null;
    }

    public Object[] values() {
        Object[] v = new Object[size];
        while (tail != null) {
            v[size - 1] = tail.getValue();
            tail = tail.left;
            size--;
        }
        return v;
    }

    public Entry<K, V> getEntry(Object key) {
        Entry<K, V> p = tail;
        if (comparator != null) {
            K k = (K) key;
            while (p != null) {
                int cmp = comparator.compare(k, p.key);
                if (cmp < 0)
                    p = p.left;
                else if (cmp == 0)
                    return p;
                else
                    return null;
            }
            return null;
        } else {
            if (key == null)
                throw new NullPointerException();
            Comparable<? super K> k = (Comparable<? super K>) key;
            while (p != null) {
                int cmp = k.compareTo(p.key);
                if (cmp < 0)
                    p = p.left;
                else if (cmp == 0)
                    return p;
                else
                    return null;
            }
            return null;
        }
    }

    public void put(K key, V value) {
        Entry<K, V> t = tail;
        if (t == null) {
            compare(key, key); // type (and possibly null) check
            tail = new Entry<K, V>(key, value);
            size = 1;
            modCount++;
            return;
        }
        int cmp;
        Entry<K, V> parent = null;
        Comparator<? super K> cpr = comparator;
        if (cpr != null) {
            do {
                cmp = cpr.compare(key, t.key);
                if (cmp < 0) {
                    parent = t;
                    t = t.left;
                } else if (cmp == 0) {
                    t.setValue(value);
                    return;
                } else {
                    break;
                }
            } while (t != null);
        } else {
            if (key == null)
                throw new NullPointerException();
            Comparable<? super K> k = (Comparable<? super K>) key;
            do {
                cmp = k.compareTo(t.key);
                if (cmp < 0) {
                    parent = t;
                    t = t.left;
                } else if (cmp == 0) {
                    t.setValue(value);
                    return;
                } else {
                    break;
                }
            } while (t != null);
        }

        Entry<K, V> e = new Entry<K, V>(key, value);
        if (parent == null) {
            e.left = tail;
            tail = e;
        } else {
            parent.left = e;
            e.left = t;
        }
        parent = null;
        t = null;
        e = null;
        size++;
        modCount++;
        return;
    }

    final int compare(Object k1, Object k2) {
        return comparator == null ? ((Comparable<? super K>) k1).compareTo((K) k2) : comparator.compare((K) k1, (K) k2);
    }

    static final class Entry<K, V> implements Map.Entry<K, V> {
        K key;
        V value;
        Entry<K, V> left = null;

        /**
         * Make a new cell with given key, value, and parent, and with {@code null} child links, and BLACK color.
         */
        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Returns the key.
         *
         * @return the key
         */
        public K getKey() {
            return key;
        }

        /**
         * Returns the value associated with the key.
         *
         * @return the value associated with the key
         */
        public V getValue() {
            return value;
        }

        /**
         * Replaces the value currently associated with the key with the given
         * value.
         *
         * @return the value associated with the key before this method was
         *         called
         */
        public V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;

            return (key == null ? e.getKey() == null : key.equals(e.getKey()))
                    && (value == null ? e.getValue() == null : value.equals(e.getValue()));
        }

        public int hashCode() {
            int keyHash = (key == null ? 0 : key.hashCode());
            int valueHash = (value == null ? 0 : value.hashCode());
            return keyHash ^ valueHash;
        }

        public String toString() {
            return key + "=" + value;
        }
    }
}
