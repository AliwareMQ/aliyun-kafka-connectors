package com.aliyun.fc.kafka.connect.enums;

public enum InvocationTypeEnum {

    SYNC("SYNC", "同步调用"),
    ASYNC("ASYNC", "异步调用");

    private String value;
    private String desc;

    InvocationTypeEnum(String value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public static InvocationTypeEnum get(String value) {
        for (InvocationTypeEnum temp : InvocationTypeEnum.values()) {
            if (temp.value.equals(value)) {
                return temp;
            }
        }
        return null;
    }

    public static InvocationTypeEnum getByDesc(String desc){
        for (InvocationTypeEnum temp : InvocationTypeEnum.values()) {
            if (temp.desc.equals(desc)) {
                return temp;
            }
        }
        return null;
    }

    public String getDesc() {
        return desc;
    }

    public String getValue() {
        return value;
    }
}
