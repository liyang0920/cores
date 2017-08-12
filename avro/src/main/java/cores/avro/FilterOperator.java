package cores.avro;

public interface FilterOperator<T> {
    public String getName();

    public boolean isMatch(T t);
}
