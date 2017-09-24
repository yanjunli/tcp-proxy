package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;


import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/10.
 */
public class ActonFactory {
    String config = null;
    HashMap<String, Class<SQLAnnotation>> resMap;

    public static Map<String, Map<String, String>> pretreatmentArgs(List<Map<String, List<Map<String, String>>>> need) throws Exception {
        Iterator<Map<String, List<Map<String, String>>>> iterator = need.iterator();
        final Map<String, Map<String, String>> res = new HashMap<>();
        do {
            Map<String, List<Map<String, String>>> action = iterator.next();
            Map.Entry<String, List<Map<String, String>>> entry = action.entrySet().iterator().next();
            String actionName = entry.getKey();
            List<Map<String, String>> value;
            if ((value = entry.getValue()) != null) {
                res.put(actionName, value.stream().collect(Collectors.toMap(ConditionUtil::mappingKey, (v) -> {
                    if (v == null) {
                        return null;
                    } else {
                        return ConditionUtil.mappingValue(v);
                    }
                })));
            } else {
                res.put(actionName, null);
            }
        } while (iterator.hasNext());
        return res;
    }


    public SQLAnnotationList get(String name,List<Map<String, List<Map<String, String>>>> need) throws Exception {
        Iterator<Map.Entry<String, Map<String, String>>> iterator = pretreatmentArgs(need).entrySet().iterator();
        SQLAnnotationList annotationList=new SQLAnnotationList();
        do {
            Map.Entry<String, Map<String, String>> action = iterator.next();
            Map<String, String> args=action.getValue();
            if (args==null)args=new HashMap<>();
            args.put("matchName",name);
            String actionName = action.getKey();
            System.out.println(action.toString());
            Class<SQLAnnotation> annotationClass = resMap.get(actionName);
            SQLAnnotation annotation = annotationClass.getConstructor().newInstance();
            annotation.init(args);
            annotation.setArgs(args);
            annotation.setMethod(actionName);
            annotationList.getSqlAnnotations().add(annotation);
        } while (iterator.hasNext());
        return annotationList;
    }


    public static void main(String[] args) throws Throwable {
        ActonFactory actonFactory = new ActonFactory("actions.yaml");
        List<Map<String,List< Map<String, String>>>> list = new ArrayList<>();
        Map<String,String> monitorSQL=new HashMap<>();
        monitorSQL.put("param1","1");
        list.add(Collections.singletonMap("monitorSQL",Collections.singletonList(monitorSQL)));
        list.add(Collections.singletonMap("cacheResult",null));
        Map<String,String> sqlCach=new HashMap<>();
        sqlCach.put("param1","1");
        sqlCach.put("param2","2");
        list.add(Collections.singletonMap("sqlCach",Arrays.asList(sqlCach)));
        SQLAnnotationList annotations= actonFactory.get("default",list);
       annotations.apply(null);
    }

    public ActonFactory(String config) throws Exception {
        this.config = config.trim();
        URL url = ActonFactory.class.getClassLoader().getResource(config);
        if (url != null) {
            Yaml yaml = new Yaml();
            FileInputStream fis = new FileInputStream(url.getFile());
            Map<String, List<Map<String, String>>> obj = (Map<String, List<Map<String, String>>>) yaml.load(fis);
            Map<String, String> actions = obj.values().stream().flatMap(maps -> maps.stream()).collect(Collectors.toMap(ConditionUtil::mappingKey, ConditionUtil::mappingValue));
            resMap = new HashMap<>(actions.size());
            for (Map.Entry<String, String> it : actions.entrySet()) {
                String value = it.getValue();
                if (value == null) continue;
                Class<SQLAnnotation> k = (Class<SQLAnnotation>) ActonFactory.class.getClassLoader().loadClass(value.trim());
                resMap.put(it.getKey().trim(), k);
            }
        }
    }

    public String getConfig() {
        return config;
    }

}
