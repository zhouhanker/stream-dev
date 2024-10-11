package com.stream.common.domain;

import java.io.Serializable;

/**
 * @author han.zhou
 * @time: 2023/9/14 18:03
 * @className: HdfsInfo
 * @description 封装HDFS信息 Bean
 */
public class HdfsInfo implements Serializable {
    private String hdfsUrl;
    private boolean hdfsNeedPartition;
    private int hdfsPartitionMode;
    private String hdfsPartitionField;

    public HdfsInfo() {
    }

    public HdfsInfo(String hdfsUrl, boolean hdfsNeedPartition, int hdfsPartitionMode, String hdfsPartitionField) {
        this.hdfsUrl = hdfsUrl;
        this.hdfsNeedPartition = hdfsNeedPartition;
        this.hdfsPartitionMode = hdfsPartitionMode;
        this.hdfsPartitionField = hdfsPartitionField;
    }

    public String getHdfsUrl() {
        return hdfsUrl;
    }

    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public boolean isHdfsNeedPartition() {
        return hdfsNeedPartition;
    }

    public void setHdfsNeedPartition(boolean hdfsNeedPartition) {
        this.hdfsNeedPartition = hdfsNeedPartition;
    }

    public int getHdfsPartitionMode() {
        return hdfsPartitionMode;
    }

    public void setHdfsPartitionMode(int hdfsPartitionMode) {
        this.hdfsPartitionMode = hdfsPartitionMode;
    }

    public String getHdfsPartitionField() {
        return hdfsPartitionField;
    }

    public void setHdfsPartitionField(String hdfsPartitionField) {
        this.hdfsPartitionField = hdfsPartitionField;
    }

    @Override
    public String toString() {
        return "HdfsInfo{" +
                "hdfsUrl='" + hdfsUrl + '\'' +
                ", hdfsNeedPartition=" + hdfsNeedPartition +
                ", hdfsPartitionMode=" + hdfsPartitionMode +
                ", hdfsPartitionField='" + hdfsPartitionField + '\'' +
                '}';
    }
}
