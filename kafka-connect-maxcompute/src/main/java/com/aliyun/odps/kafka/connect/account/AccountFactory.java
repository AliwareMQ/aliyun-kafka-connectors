package com.aliyun.odps.kafka.connect.account;

public class AccountFactory<T> implements IAccountFactory<T> {

  @Override
  public <V extends T> V getGenerator(Class<V> clazz) {
    if (clazz == null) {
      return null;
    }
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
