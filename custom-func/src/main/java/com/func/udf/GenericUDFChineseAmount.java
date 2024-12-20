package com.func.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @Package com.func.udf.GenericUDFChineseAmount
 * @Author zhou.han
 * @Date 2024/12/15 23:11
 * @description: 转换财务数字
 */
@Description(name = "convert_number_chinese_amount", value = "Chinese Finance Convert")
public class GenericUDFChineseAmount extends GenericUDF {

    private static final String[] CN_UPPER_NUMBER = { "零", "壹", "贰", "叁", "肆",
            "伍", "陆", "柒", "捌", "玖" };
    private static final String[] CN_UNIT = { "", "拾", "佰", "仟", "万", "拾", "佰", "仟", "亿", "拾",
            "佰", "仟", "万", "拾", "佰", "仟", "元" };
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentLengthException("This is args is only one");
        }
        if (!(objectInspectors[0] instanceof PrimitiveObjectInspector)){
            throw new UDFArgumentTypeException(0, "The argument of function double_to_chinese should be a numeric type.");
        }
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) objectInspectors[0];
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE
                && inputOI.getPrimitiveCategory()!= PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
            throw new UDFArgumentTypeException(0, "The argument of function double_to_chinese should be a double or decimal type.");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0] == null) {
            return null;
        }
        Object inputObj = deferredObjects[0].get();
        if (inputObj == null) {
            return null;
        }
        double inputValue;
        if (inputObj instanceof BigDecimal) {
            inputValue = ((BigDecimal) inputObj).doubleValue();
        } else {
            inputValue = (double) inputObj;
        }
        StringBuilder chineseAmount = new StringBuilder();

        BigDecimal bd = new BigDecimal(inputValue);
        bd = bd.setScale(2, RoundingMode.HALF_UP);
        long integerPart = bd.longValue();
        int unitIndex = 0;
        boolean zeroFlag = false;  // 标记是否连续出现零
        while (integerPart > 0) {
            int digit = (int) (integerPart % 10);
            if (digit == 0) {
                zeroFlag = true;
            } else {
                if (zeroFlag) {
                    chineseAmount.insert(0, CN_UPPER_NUMBER[0]);
                    zeroFlag = false;
                }
                chineseAmount.insert(0, CN_UPPER_NUMBER[digit] + CN_UNIT[unitIndex]);
            }
            integerPart /= 10;
            unitIndex++;
        }

        int decimalPart = bd.remainder(BigDecimal.ONE).multiply(new BigDecimal(100)).intValue();
        if (decimalPart > 0) {
            chineseAmount.append("元");
            if (decimalPart >= 10) {
                chineseAmount.append(CN_UPPER_NUMBER[decimalPart / 10]).append("角");
            }
            if (decimalPart % 10 > 0) {
                chineseAmount.append(CN_UPPER_NUMBER[decimalPart % 10]).append("分");
            }
        } else {
            chineseAmount.append("元整");
        }

        return new Text(chineseAmount.toString());
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "convert_number_chinese_amount(" + strings[0] + ")";
    }
}
