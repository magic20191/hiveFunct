package org.example.udf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * 一进一出
 * User-Defined-Function
 * 自定义一个函数，自定义UDF函数判断员工表中的工资级别
 * 接收一个参数
 *
     任务1：自定义UDF函数判断员工表中的工资级别。
     · sal <= 1000，工资级别为Grade_A；
     · 1000 < sal <= 3000，工资级别为Grade_B；
     · 3000 < sal，工资级别为Grade_C
 */
public class salesLevel extends GenericUDF {

    /**
     * 执行一次 检查参数个数 和 参数类型
     **/
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1){
            throw new UDFArgumentException("直接收一个参数");
        }
        ObjectInspector outputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return outputOI;
    }


    /**
     * 处理数据
     **/
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        if(arguments[0].get() != null){
            try {
                Object obj = arguments[0].get();

                Double temp;

                if (obj instanceof IntWritable) {
                    int number = ((IntWritable) obj).get();
                    temp = (double) number;
                } else if (obj instanceof HiveDecimalWritable) {
                    double number = ((HiveDecimalWritable) obj).doubleValue();
                    temp =  number;
                } else if (obj instanceof Text) {
                    int number = Integer.parseInt(((Text) obj).toString());
                    temp = (double) number;
                } else {
                      return "参数异常";
                }

                if( temp <= 1000 ){
                    return "Grade_A";
                } else if( 1000 < temp && temp <= 3000) {
                    return "Grade_B";
                } else if (temp > 3000) {
                    return "Grade_C";
                }else{
                    return "NULL";
                }

            }catch (Exception e){
                return "obj is error";
            }
        }
        return "NULL";
    }

    /**
     * explain 详细计划 说明
     **/
    public String getDisplayString(String[] strings) {
        return "· 薪水 <= 1000，工资级别为Grade_A；\n" +
                "\n" +
                "· 1000 < 薪水 <= 3000，工资级别为Grade_B；\n" +
                "\n" +
                "· 3000 < 薪水，工资级别为Grade_C。";
    }

}
