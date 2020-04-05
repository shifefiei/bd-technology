package org.bd.hadoop.mapReduce.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shifeifei on 2020/4/2.
 */
public class MobileFlow implements Writable {

    private String phoneNo;
    private Long uploadFlow;
    private Long downLoadFlow;
    private Long sumFlow;

    public MobileFlow() {
    }

    //写序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(uploadFlow);
        out.writeLong(downLoadFlow);
        out.writeLong(sumFlow);
    }

    //读序列化
    public void readFields(DataInput in) throws IOException {
        this.uploadFlow = in.readLong();
        this.downLoadFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public String getPhoneNo() {
        return phoneNo;
    }

    public void setPhoneNo(String phoneNo) {
        this.phoneNo = phoneNo;
    }

    public Long getUploadFlow() {
        return uploadFlow;
    }

    public void setUploadFlow(Long uploadFlow) {
        this.uploadFlow = uploadFlow;
    }

    public Long getDownLoadFlow() {
        return downLoadFlow;
    }

    public void setDownLoadFlow(Long downLoadFlow) {
        this.downLoadFlow = downLoadFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return uploadFlow + "," + downLoadFlow + "," + sumFlow;
    }
}
