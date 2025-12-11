package com.bigdata.spark.model;

import java.io.Serializable;

/**
 * 带标签的记录
 */
public class TaggedRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String devSerial;
    private String tagResult;
    private String dt;

    public TaggedRecord() {
    }

    public TaggedRecord(String devSerial, String tagResult, String dt) {
        this.devSerial = devSerial;
        this.tagResult = tagResult;
        this.dt = dt;
    }

    public String getDevSerial() {
        return devSerial;
    }

    public void setDevSerial(String devSerial) {
        this.devSerial = devSerial;
    }

    public String getTagResult() {
        return tagResult;
    }

    public void setTagResult(String tagResult) {
        this.tagResult = tagResult;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    @Override
    public String toString() {
        return "TaggedRecord{" +
                "devSerial='" + devSerial + '\'' +
                ", tagResult='" + tagResult + '\'' +
                ", dt='" + dt + '\'' +
                '}';
    }
}
