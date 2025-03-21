package com.stream.utils;

import static com.stream.utils.SiliconFlowApi.generateBadReview;

/**
 * @Package com.stream.utils.CommonGenerateTempLate
 * @Author zhou.han
 * @Date 2025/3/16 19:43
 * @description: TempLate
 */
public class CommonGenerateTempLate {

    private static final String TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";
    private static final String COMMENT = "差评";

    private static final String API_TOKEN = "sk-pfaasdbkktgnunlvoapothvybwflwdteckwmhakzqbthcgnj";

    public static String GenerateComment(String comment,String productName){
        if (comment.equals(COMMENT)){
            return generateBadReview(
                    String.format(TEMPLATE,COMMENT, productName, "攻击性拉满,使用脏话"),
                    API_TOKEN
            );
        }
        return generateBadReview(
                String.format(TEMPLATE,COMMENT, productName,""),
                API_TOKEN
        );
    }

}
