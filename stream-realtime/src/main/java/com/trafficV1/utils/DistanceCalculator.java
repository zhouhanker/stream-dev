package com.trafficV1.utils;

import java.math.BigDecimal;
import java.math.MathContext;

public class DistanceCalculator {
    // WGS84椭球参数
    private static final double a = 6378137.0; // 半长轴(米)
    private static final double b = 6356752.314245; // 半短轴(米)
    private static final double f = 1 / 298.257223563; // 扁率

    /**
     * 计算两个经纬度点之间的距离(米)
     * @param lon1 第一个点的经度
     * @param lat1 第一个点的纬度
     * @param lon2 第二个点的经度
     * @param lat2 第二个点的纬度
     * @return 两点之间的距离(米)
     */
    public static double calculateDistance(double lon1, double lat1, double lon2, double lat2) {
        // 将角度转换为弧度
        double L = Math.toRadians(lon2 - lon1);
        double U1 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat1)));
        double U2 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat2)));

        double sinU1 = Math.sin(U1);
        double cosU1 = Math.cos(U1);
        double sinU2 = Math.sin(U2);
        double cosU2 = Math.cos(U2);

        double lambda = L;
        double lambdaP = 2 * Math.PI;
        int iterLimit = 100;

        double sinLambda = 0;
        double cosLambda = 0;
        double sinSigma = 0;
        double cosSigma = 0;
        double sigma = 0;
        double sinAlpha = 0;
        double cosSqAlpha = 0;
        double cos2SigmaM = 0;

        // 迭代计算
        while (Math.abs(lambda - lambdaP) > 1e-12 && --iterLimit > 0) {
            sinLambda = Math.sin(lambda);
            cosLambda = Math.cos(lambda);

            sinSigma = Math.sqrt((cosU2 * sinLambda) * (cosU2 * sinLambda) +
                    (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) *
                            (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda));

            if (sinSigma == 0) {
                return 0.0; // 两点重合
            }

            cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
            sigma = Math.atan2(sinSigma, cosSigma);

            sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
            cosSqAlpha = 1 - sinAlpha * sinAlpha;
            cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha;

            if (Double.isNaN(cos2SigmaM)) {
                cos2SigmaM = 0; // 当cosSqAlpha为0时，即接近赤道
            }

            double C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
            lambdaP = lambda;
            lambda = L + (1 - C) * f * sinAlpha *
                    (sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));
        }

        if (iterLimit == 0) {
            return Double.NaN; // 迭代未收敛
        }

        // 计算距离
        double uSq = cosSqAlpha * (a * a - b * b) / (b * b);
        double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
        double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));

        double deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) -
                B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));

        double s = b * A * (sigma - deltaSigma);

        // 转换为米并四舍五入到小数点后两位
        return new BigDecimal(s, new MathContext(8)).doubleValue();
    }

    public static void main(String[] args) {
        // 坐标1: (101.717055, 25.003066)
        double lon1 = 101.717055;
        double lat1 = 25.003066;

        // 坐标2: (106.73288, 22.061094)
        double lon2 = 106.73288;
        double lat2 = 22.061094;

        double distance = calculateDistance(lon1, lat1, lon2, lat2);

        System.out.println("两点之间的距离: " + distance + " 米");
        System.out.println("两点之间的距离: " + distance / 1000 + " 公里");
    }
}
