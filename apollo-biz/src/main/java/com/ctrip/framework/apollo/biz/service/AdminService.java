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
package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Cluster;
import com.ctrip.framework.apollo.common.entity.App;
import com.ctrip.framework.apollo.core.ConfigConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Service
public class AdminService {
  private final static Logger logger = LoggerFactory.getLogger(AdminService.class);

  private final AppService appService;
  private final AppNamespaceService appNamespaceService;
  private final ClusterService clusterService;
  private final NamespaceService namespaceService;

  public AdminService(
      final AppService appService,
      final @Lazy AppNamespaceService appNamespaceService,
      final @Lazy ClusterService clusterService,
      final @Lazy NamespaceService namespaceService) {
    this.appService = appService;
    this.appNamespaceService = appNamespaceService;
    this.clusterService = clusterService;
    this.namespaceService = namespaceService;
  }

  /**
   * 创建一个新的APP
   * <ol>
   *     <li>
   *         创建APP {@link AppService#save(com.ctrip.framework.apollo.common.entity.App)}
   *     </li>
   *     <li>
   *         创建默认的AppNamespace模板 {@link AppNamespaceService#createDefaultAppNamespace(String, String)}
   *     </li>
   *     <li>
   *         创建默认的集群default {@link ClusterService#createDefaultCluster(String, String)}
   *     </li>
   *     <li>
   *         根据 AppNamespace 模板创建对应的 Namespace {@link NamespaceService#instanceOfAppNamespaces(String, String, String)}
   *     </li>
   * </ol>
   * @param app
   * @return
   */
  @Transactional
  public App createNewApp(App app) {
    String createBy = app.getDataChangeCreatedBy();
    App createdApp = appService.save(app);

    String appId = createdApp.getAppId();

    appNamespaceService.createDefaultAppNamespace(appId, createBy);

    clusterService.createDefaultCluster(appId, createBy);

    namespaceService.instanceOfAppNamespaces(appId, ConfigConsts.CLUSTER_NAME_DEFAULT, createBy);

    return app;
  }

  /**
   * 删除APP
   * <ol>
   *     <li>
   *         查询到所有的集群，然后挨个删除这些集群及其相关的Namespace {@link ClusterService#findParentClusters(String)} <br/>
   *         注意，这里是删除的主集群。关于这个主集群，参见 <a href="https://www.cnblogs.com/deepSleeping/p/14565804.html">深入理解Apollo核心机制之灰度发布——创建灰度</a>
   *     </li>
   *     <li>
   *        删除该应用下所有的Namespace模板 即AppNamespace {@link AppNamespaceService#batchDelete(String, String)}
   *     </li>
   *     <li>
   *         最后删除APP {@link AppService#delete(long, String)}
   *     </li>
   * </ol>
   *
   * @param app       要删除的APP
   * @param operator  操作人
   */
  @Transactional
  public void deleteApp(App app, String operator) {
    String appId = app.getAppId();

    logger.info("{} is deleting App:{}", operator, appId);

    List<Cluster> managedClusters = clusterService.findParentClusters(appId);

    // 1. delete clusters
    if (Objects.nonNull(managedClusters)) {
      for (Cluster cluster : managedClusters) {
        clusterService.delete(cluster.getId(), operator);
      }
    }

    // 2. delete appNamespace
    appNamespaceService.batchDelete(appId, operator);

    // 3. delete app
    appService.delete(app.getId(), operator);
  }
}
