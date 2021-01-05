package com.aliyun.odps.kafka.connect.account.impl;

import java.util.Map;

import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;
import com.aliyun.odps.kafka.connect.account.AccountGenerator;
import com.aliyun.odps.kafka.connect.account.sts.StsService;
import com.aliyun.odps.kafka.connect.account.sts.StsUserBo;

public class STSAccountGenerator implements AccountGenerator<StsAccount> {

  private final StsService stsService = new StsService();

  @Override
  public StsAccount generate(MaxComputeSinkConnectorConfig config) {
    Map<String, String> env = System.getenv();
    String ak = env.getOrDefault("ACCESS_ID", "");
    String sk = env.getOrDefault("ACCESS_KEY", "");

    String accountId = config.getString(MaxComputeSinkConnectorConfig.ACCOUNT_ID);
    String regionId = config.getString(MaxComputeSinkConnectorConfig.REGION_ID);
    String roleName = config.getString(MaxComputeSinkConnectorConfig.ROLE_NAME);
    String stsEndpoint = config.getString(MaxComputeSinkConnectorConfig.STS_ENDPOINT);
    StsUserBo stsUserBo = stsService.getAssumeRole(accountId, regionId, stsEndpoint, ak, sk, roleName);
    String token = stsUserBo.getToken();
    String id = stsUserBo.getAk();
    String key = stsUserBo.getSk();
    return new StsAccount(id, key, token);
  }
}
