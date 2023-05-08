/*
 * Copyright 2023 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.portal.component;


import com.ctrip.framework.apollo.portal.environment.PortalMetaDomainService;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.portal.api.AdminServiceAPI;
import com.ctrip.framework.apollo.portal.component.config.PortalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>主要负责检测各个环境是否可用</p>
 * <p>检测方式即通过AdminService的健康检查机制来判断是否可用</p>
 */
@Component
public class PortalSettings {

  private static final Logger logger = LoggerFactory.getLogger(PortalSettings.class);
  /**
   * 健康检查的时间，10秒钟检查一次
   */
  private static final int HEALTH_CHECK_INTERVAL = 10 * 1000;

  private final ApplicationContext applicationContext;
  /**
   * Portal服务的配置，含有所有的相关配置
   */
  private final PortalConfig portalConfig;

  /**
   * 用于获取MetaService地址
   */
  private final PortalMetaDomainService portalMetaDomainService;

  private List<Env> allEnvs = new ArrayList<>();

  /**
   * 用来标记环境是否可用的map，key为环境，value为这个环境是否可用，true为可用，false为不可用
   */
  //mark env up or down
  private Map<Env, Boolean> envStatusMark = new ConcurrentHashMap<>();

  public PortalSettings(
          final ApplicationContext applicationContext,
          final PortalConfig portalConfig,
          final PortalMetaDomainService portalMetaDomainService
  ) {
    this.applicationContext = applicationContext;
    this.portalConfig = portalConfig;
    this.portalMetaDomainService = portalMetaDomainService;
  }

  /**
   * 在启动的时候会从配置中获取到可用的所有环境，这个读取没有刷新的功能，也就是说，在Portal启动的时候就确定了所有的ENV环境了，如果增加修改删除了某个环境，必须要重启Portal服务才能生效
   */
  @PostConstruct
  private void postConstruct() {

    allEnvs = portalConfig.portalSupportedEnvs();

    for (Env env : allEnvs) {
      envStatusMark.put(env, true);
    }

    ScheduledExecutorService
        healthCheckService =
        Executors.newScheduledThreadPool(1, ApolloThreadFactory.create("EnvHealthChecker", true));

    healthCheckService
        .scheduleWithFixedDelay(new HealthCheckTask(applicationContext), 1000, HEALTH_CHECK_INTERVAL,
                                TimeUnit.MILLISECONDS);

  }

  public List<Env> getAllEnvs() {
    return allEnvs;
  }

  public List<Env> getActiveEnvs() {
    List<Env> activeEnvs = new LinkedList<>();
    for (Env env : allEnvs) {
      if (envStatusMark.get(env)) {
        activeEnvs.add(env);
      }
    }
    return activeEnvs;
  }

  public boolean isEnvActive(Env env) {
    Boolean mark = envStatusMark.get(env);
    return mark != null && mark;
  }

  /**
   * 用于检查某个环境是否可用，本质上就是检查某个环境的AdminService是否健康
   */
  private class HealthCheckTask implements Runnable {

    private static final int ENV_DOWN_THRESHOLD = 2;

    private Map<Env, Integer> healthCheckFailedCounter = new HashMap<>();

    private AdminServiceAPI.HealthAPI healthAPI;

    public HealthCheckTask(ApplicationContext context) {
      healthAPI = context.getBean(AdminServiceAPI.HealthAPI.class);
      for (Env env : allEnvs) {
        healthCheckFailedCounter.put(env, 0);
      }
    }

    public void run() {

      for (Env env : allEnvs) {
        try {
          if (isUp(env)) {
            //revive
            if (!envStatusMark.get(env)) {
              envStatusMark.put(env, true);
              healthCheckFailedCounter.put(env, 0);
              logger.info("Env revived because env health check success. env: {}", env);
            }
          } else {
            logger.error("Env health check failed, maybe because of admin server down. env: {}, meta server address: {}", env,
                    portalMetaDomainService.getDomain(env));
            handleEnvDown(env);
          }

        } catch (Exception e) {
          logger.error("Env health check failed, maybe because of meta server down "
                       + "or configure wrong meta server address. env: {}, meta server address: {}", env,
                  portalMetaDomainService.getDomain(env), e);
          handleEnvDown(env);
        }
      }

    }

    private boolean isUp(Env env) {
      Health health = healthAPI.health(env);
      return "UP".equals(health.getStatus().getCode());
    }

    private void handleEnvDown(Env env) {
      int failedTimes = healthCheckFailedCounter.get(env);
      healthCheckFailedCounter.put(env, ++failedTimes);

      if (!envStatusMark.get(env)) {
        logger.error("Env is down. env: {}, failed times: {}, meta server address: {}", env, failedTimes,
                portalMetaDomainService.getDomain(env));
      } else {
        if (failedTimes >= ENV_DOWN_THRESHOLD) {
          envStatusMark.put(env, false);
          logger.error("Env is down because health check failed for {} times, "
                       + "which equals to down threshold. env: {}, meta server address: {}", ENV_DOWN_THRESHOLD, env,
                  portalMetaDomainService.getDomain(env));
        } else {
          logger.error(
              "Env health check failed for {} times which less than down threshold. down threshold:{}, env: {}, meta server address: {}",
              failedTimes, ENV_DOWN_THRESHOLD, env, portalMetaDomainService.getDomain(env));
        }
      }

    }

  }
}
