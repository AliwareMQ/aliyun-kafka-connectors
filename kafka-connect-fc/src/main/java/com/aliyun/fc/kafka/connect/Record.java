package com.aliyun.fc.kafka.connect;

public class Record {
    private Long timestamp;
    private String topic;
    private int partition;
    private long offset;
    private Object key;
    private Object value;
    private boolean overflowFlag;
    private long valueSize;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isOverflowFlag() {
        return overflowFlag;
    }

    public void setOverflowFlag(boolean overflowFlag) {
        this.overflowFlag = overflowFlag;
    }

    public long getValueSize() {
        return valueSize;
    }

    public void setValueSize(long valueSize) {
        this.valueSize = valueSize;
    }
}
