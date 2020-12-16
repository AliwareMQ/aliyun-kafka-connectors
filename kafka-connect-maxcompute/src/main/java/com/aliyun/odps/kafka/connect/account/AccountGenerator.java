package com.aliyun.odps.kafka.connect.account;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;

public interface AccountGenerator<T extends Account> {

  T generate(MaxComputeSinkConnectorConfig config);
}
