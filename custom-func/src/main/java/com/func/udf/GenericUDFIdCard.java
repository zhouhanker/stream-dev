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

import java.util.regex.Pattern;

/**
 * @Package com.func.udf.GenericUDFIdCard
 * @Author zhou.han
 * @Date 2024/11/14 00:22
 * @description: 身份证校验
 */
@Description(name = "validate_id_card", value = "A custom UDF to validate Chinese ID Card number")
public class GenericUDFIdCard extends GenericUDF {

    private static final int[] WEIGHT_FACTOR = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
    private static final char[] CHECK_CODE = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!= 1) {
            throw new UDFArgumentLengthException("The function validate_id_card expects exactly 1 argument.");
        }
        // 检查输入参数类型是否为字符串类型（Text类型在Hive底层对应的ObjectInspector类型判断）
        if (!(objectInspectors[0] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The argument of function validate_id_card should be a string.");
        }
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) objectInspectors[0];
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "The argument of function validate_id_card should be a string.");
        }

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0] == null) {
            return null;
        }

        Text inputText = (Text) deferredObjects[0].get();
        if (inputText == null) {
            return null;
        }

        String idCardStr = inputText.toString().trim();
        if (!isValidLength(idCardStr)) {
            return new Text("无效长度");
        }
        if (!isValidFormat(idCardStr)) {
            return new Text("格式错误");
        }
        if (!checkVerifyCode(idCardStr)) {
            return new Text("校验码错误");
        }
        return new Text("有效身份证");
    }

    private boolean isValidLength(String idCard) {
        return idCard.length() == 18;
    }

    private boolean isValidFormat(String idCard) {
        String regex = "(^[1-9]\\d{5}(18|19|20)\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|" +
                "(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}$)";
        return Pattern.matches(regex, idCard);
    }

    private boolean checkVerifyCode(String idCard) {
        int sum = 0;
        for (int i = 0; i < 17; i++) {
            sum += (idCard.charAt(i) - '0') * WEIGHT_FACTOR[i];
        }
        int remainder = sum % 11;
        return CHECK_CODE[remainder] == idCard.charAt(17);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "validate_id_card(" + strings[0] + ")";
    }
}
