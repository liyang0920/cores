package cores.avro.mapreduce;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationUtil {
    public static void setClass(String key, Configuration conf, Object userDefineObject) {
        String userStr = com.alibaba.fastjson.JSON.toJSON(userDefineObject).toString();
        conf.set(key, userStr);
    }

    public static Object getClass(String key, Configuration conf, Class<?> classType) {
        String str = conf.get(key);
        Object object = com.alibaba.fastjson.JSON.parseObject(str, classType);
        return object;
    }
}
