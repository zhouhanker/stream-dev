package com.stream.common.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author weikaijun
 * @date 2021-09-27 09:52
 **/
public class DingTalkUtils {
    private static List<String> BIG_DATA_GROUP_USER = Arrays.asList("15906211002", "18625256191", "13815404021");

    private static final Logger logger = LoggerFactory.getLogger(DingTalkUtils.class);

    public static void dingMsg(String msg, List<String> mobileList, MsgGroup msgGroup) {
        try {
            String accessToken = msgGroup.getAccToken();
            Long timestamp = System.currentTimeMillis();

            //额外处理大数据组同学
            if (!MsgGroup.BIG_DATA_GROUP.equals(msgGroup) && mobileList.stream().anyMatch(x -> BIG_DATA_GROUP_USER.contains(x))) {
                dingMsg(msg, mobileList, MsgGroup.BIG_DATA_GROUP);
                return;
            }

            DingDingMsgRequest dingDingMsgRequest = new DingDingMsgRequest();
            dingDingMsgRequest.setText(msg);
            if (CollectionUtil.isNotEmpty(mobileList)) {
                dingDingMsgRequest.setAt(mobileList);
            }
            logger.info("bigDataGroupMsg[request]：" + JSONObject.toJSONString(dingDingMsgRequest));
            String res = HttpUtil.post(
                    "https://oapi.dingtalk.com/robot/send?access_token=" + accessToken
                            + "&timestamp=" + timestamp.toString()
                            + "&sign=" + getSign(timestamp, msgGroup.getSecret()),
                    JSONObject.toJSONString(dingDingMsgRequest)
            );
            logger.info("bigDataGroupMsg[response]：" + res);
        } catch (Exception e) {
            if (e.getMessage().contains("refused")) {
                try {
                    Map<String, Object> param = new HashMap<>();
                    param.put("msg", msg);
                    param.put("mobileList", mobileList);
                    HttpUtil.post("http://node03:50901/open/dingTalk", JSONObject.toJSONString(param));
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("钉钉消息转发失败：" + e.getMessage());
                }
            } else {
                logger.error("钉钉消息发送失败：" + e.getMessage());
            }
        }
    }

    private static String getSign(Long timestamp, String secret) {
        try {
            String stringToSign = timestamp + "" + secret;
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] signData = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
            return URLEncoder.encode(new String(Base64.getEncoder().encode(signData)), "UTF-8");
        } catch (Exception e) {
            logger.error("加签失败" + e.getMessage());
            e.printStackTrace();
            return "";
        }
    }

    @Data
    private static class DingDingMsgRequest {
        private String msgtype = "text";
        private DingDingMsgContent text = new DingDingMsgContent();
        private DingDingMsgAt at = new DingDingMsgAt();

        public void setText(String msg) {
            this.text.setContent(msg);
        }

        public void setAt(List<String> mobiles) {
            this.at.setAtMobiles(mobiles);
        }
    }

    @Data
    private static class DingDingMsgContent {
        private String content;
    }

    @Data
    private static class DingDingMsgAt {
        private List<String> atMobiles;
    }

    @Getter
    public enum MsgGroup {
        /**
         * 消息组
         */
        COMMON(0, "通用",
                "0675aa5014fd8d00f9e039d07b65efc8c3d546a448b99951e08e6752895bf115",
                "SEC422a7427d223fb0cdaea5365aa0fbce7f7569fe93b8336a90e483d446c5cac0f"),
        BIG_DATA_GROUP(1, "大数据组",
                "1d04c8556931b18337396c58082c46d5a9a3d77837080073c767ae1eb0d08e87",
                "SEC365b06ed8f3fb532c74cfbfac8277b0a390b99ce6f3b4f94f5ff9883d7e869cd"),
        ;

        private int code;
        private String groupName;
        private String accToken;
        private String secret;

        MsgGroup(int code, String groupName, String accToken, String secret) {
            this.code = code;
            this.groupName = groupName;
            this.accToken = accToken;
            this.secret = secret;
        }
    }
}
