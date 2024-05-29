package org.example.udaf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

/**
 * 该类注册UDAF到hive时用到的，负责实例化 UDAFEvaluator，给hive使用：
 *
 * 总的功能 自定义UDAF函数返回该列中所有字符串的字符总数。
 *
 */
public class charCountResolver extends AbstractGenericUDAFResolver {

    /**
     * 该方法会根据sql传人的参数数据格式指定调用哪个Evaluator进行处理。
     * @param parameters
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        return new charCountEvaluator();
    }


    /**
     * 这里是UDAF的实际处理类
     */
    public static class charCountEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        int total = 0;

        private boolean warned = false;


        /**
         * 存储当前字符总数的类
         */
        static class charsSumAgg implements AggregationBuffer {
            int sum = 0;
            void add(int num){
                sum += num;
            }
        }

        /**
         * 每个阶段都会被执行的方法，
         * 确定各个阶段输入输出参数的数据格式ObjectInspectors
         * 这里面主要是把每个阶段要用到的输入输出inspector好，其他方法被调用时就能直接使用了
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

            assert(parameters.length == 1);
            super.init(m, parameters);

            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //其余阶段，输入为Integer基础数据格式
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // 指定各个阶段输出数据格式都为Integer类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;
        }

        /**
         * 保存数据聚集结果的类
         * @return
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {

            charsSumAgg result = new charsSumAgg();
            return result;
        }

        /**
         * 重置聚集结果
         * 总数清理
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            charsSumAgg myagg = new charsSumAgg();
        }


        /**
         * 不断被调用执行的方法，最终数据都保存在agg中
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if( parameters.length != 1) {
                throw new UDFArgumentException("直接收一个参数");
            }

            if (parameters[0] != null) {
                charsSumAgg myagg = (charsSumAgg) agg;
                Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
                myagg.add(String.valueOf(p1).length());
            }
        }

        /**
         * map与combiner结束返回结果，得到部分数据聚集结果
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            charsSumAgg myagg = (charsSumAgg) agg;
            total += myagg.sum;
            return total;
        }

        /**
         * combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
         * @param agg
         * @param partition
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partition) throws HiveException {

            if (partition != null) {

                charsSumAgg myagg1 = (charsSumAgg) agg;

                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partition);

                charsSumAgg myagg2 = new charsSumAgg();

                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);
            }

        }

        /**
         * reducer阶段，输出最终结果
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            charsSumAgg myagg = (charsSumAgg) agg;
            total = myagg.sum;
            return myagg.sum;
        }

    }


    }























































//public static enum Mode {
//    /**
//     * PARTIAL1: 这个是mapreduce的map阶段:从原始数据到部分数据聚合
//     * 将会调用iterate()和terminatePartial()
//     */
//    PARTIAL1,
//    /**
//     * PARTIAL2: 这个是mapreduce的map端的Combiner阶段，负责在map端合并map的数据::从部分数据聚合到部分数据聚合:
//     * 将会调用merge() 和 terminatePartial()
//     */
//    PARTIAL2,
//    /**
//     * FINAL: mapreduce的reduce阶段:从部分数据的聚合到完全聚合
//     * 将会调用merge()和terminate()
//     */
//    FINAL,
//    /**
//     * COMPLETE: 如果出现了这个阶段，表示mapreduce只有map，没有reduce，所以map端就直接出结果了:从原始数据直接到完全聚合
//     * 将会调用 iterate()和terminate()
//     */
//    COMPLETE
//};


//https://blog.csdn.net/weixin_38750084/article/details/104567575
//https://www.cnblogs.com/bolingcavalry/p/14988931.html