package com.aliyun.odps.kafka.connect.account;

public interface IAccountFactory<T> {

  <V extends T> V getGenerator(Class<V> clazz);
}
