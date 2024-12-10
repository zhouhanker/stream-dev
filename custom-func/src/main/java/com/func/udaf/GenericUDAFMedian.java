package com.func.udaf;




import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @Package com.func.udaf.GenericUDAFMedian
 * @Author zhou.han
 * @Date 2024/11/13 22:55
 * @description: UDAF Func Median Hive version -> hive 2.1.1 of cdh6.3.1
 */
public class GenericUDAFMedian extends GenericUDAFAverage {
    public GenericUDAFMedian() {
        super();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return super.getEvaluator(parameters);
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
        return super.getEvaluator(paramInfo);
    }
}
