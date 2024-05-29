package org.example.utdf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
     任务3：自定义UDTF函数用来解析key:value:value形式的字符串。
     原数据：1:a:A;2:b:B;3:c:C;4:d:D;
     结果数据：
     id lower upper
     1   a   A
     2   b   B
     3   c   C
     4   d   D
 *
 */
public class sentenceSplit extends GenericUDTF {

    private  ArrayList<ArrayList<String>> outPutList= new ArrayList<ArrayList<String>>();

    /**
     * initialize方法是针对整个任务调一次，initialize作用是定义输出字段的列名、和输出字段的数据类型
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {


        //输出数据的默认别名,可以被别名覆盖
        List<String> fieldNames=new ArrayList<String>();
        //输出数据的类型
        List<ObjectInspector> valueInspectors = new ArrayList<>();

        fieldNames.add("id");
        valueInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("lower");
        valueInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("upper");
        valueInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);


        //最终返回值
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, valueInspectors);
    }

    /**
     * 处理输入数据
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        if(args.length != 1){
            throw new UDFArgumentException("直接收一个参数");
        }

        String line = args[0].toString();
        String[] lines = line.split(";");

        for (int i = 0; i < lines.length; i++) {
            try {
                String[] cols = lines[i].split(":");
                if(cols != null && cols.length != 0){
                    forward(cols);
                }
            } catch (Exception e) {
                continue;
            }
        }
    }

    /**
     * 收尾方法
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }

}



























//https://blog.csdn.net/m0_74120525/article/details/135092803
//https://www.imooc.com/article/36396