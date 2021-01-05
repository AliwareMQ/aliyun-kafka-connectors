package com.aliyun.odps.kafka.connect.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.AccountFactory;
import com.aliyun.odps.kafka.connect.account.AccountGenerator;
import com.aliyun.odps.kafka.connect.account.IAccountFactory;
import com.aliyun.odps.kafka.connect.account.impl.AliyunAccountGenerator;
import com.aliyun.odps.kafka.connect.account.impl.STSAccountGenerator;

public class OdpsUtils {

  private static final IAccountFactory<AccountGenerator<?>> accountFactory = new AccountFactory<>();

  public static Odps getOdps(MaxComputeSinkConnectorConfig config) {
    String accountType = config.getString(MaxComputeSinkConnectorConfig.ACCOUNT_TYPE);
    Account account;
    if (accountType.equalsIgnoreCase(Account.AccountProvider.STS.toString())) {
      account = accountFactory.getGenerator(STSAccountGenerator.class).generate(config);
    } else if (accountType.equalsIgnoreCase(Account.AccountProvider.ALIYUN.toString())) {
      account = accountFactory.getGenerator(AliyunAccountGenerator.class).generate(config);
    } else {
      throw new RuntimeException(
          String.format("Please check your ACCOUNT_TYPE config. Current: [%s].", accountType));
    }

    Odps odps = new Odps(account);
    String endpoint = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT);
    String project = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    return odps;
  }
}
