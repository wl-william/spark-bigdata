package com.bigdata.spark.model;

import java.io.Serializable;

/**
 * 视频文本记录
 */
public class VideoTextRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String devSerial;
    private String videoTextMerged;

    public VideoTextRecord() {
    }

    public VideoTextRecord(String devSerial, String videoTextMerged) {
        this.devSerial = devSerial;
        this.videoTextMerged = videoTextMerged;
    }

    public String getDevSerial() {
        return devSerial;
    }

    public void setDevSerial(String devSerial) {
        this.devSerial = devSerial;
    }

    public String getVideoTextMerged() {
        return videoTextMerged;
    }

    public void setVideoTextMerged(String videoTextMerged) {
        this.videoTextMerged = videoTextMerged;
    }

    @Override
    public String toString() {
        return "VideoTextRecord{" +
                "devSerial='" + devSerial + '\'' +
                ", videoTextMerged='" + (videoTextMerged != null ?
                    videoTextMerged.substring(0, Math.min(50, videoTextMerged.length())) + "..." :
                    "null") +
                '\'' +
                '}';
    }
}
