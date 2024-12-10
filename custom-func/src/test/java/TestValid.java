import com.func.udf.GenericUDFIdCard;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Package PACKAGE_NAME.Test
 * @Author zhou.han
 * @Date 2024/11/14 00:42
 * @description: Test
 */
public class TestValid {


    @Test
    public void testValidateIdCard() throws UDFArgumentException, HiveException {
        GenericUDFIdCard udf = new GenericUDFIdCard();

        // 创建输入参数对应的ObjectInspector，模拟传入一个字符串类型的身份证号码参数
        ObjectInspector inputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        ObjectInspector[] inputOIs = {inputOI};

        // 初始化UDF，获取输出结果对应的ObjectInspector
        ObjectInspector outputOI = udf.initialize(inputOIs);

        // 准备要校验的身份证号码，这里使用一个示例号码，实际中可替换为不同测试用例的号码
        Text inputText = new Text("320924199712065272");
        System.err.println(inputText);
        Object[] inputArgs = {inputText};

        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[1];
        deferredObjects[0] = new GenericUDF.DeferredObject() {
            @Override
            public void prepare(int i) throws HiveException {
                System.err.println("prepare 方法执行");
                System.err.println(i);
            }

            @Override
            public Object get() {
                return inputText;
            }
        };

        // 调用evaluate方法进行校验并获取结果
        Text result = (Text) udf.evaluate(deferredObjects);

        // 进行断言验证结果是否符合预期，这里简单示例，可根据实际期望结果完善断言逻辑
        assertEquals("有效身份证", result.toString());

    }

}
