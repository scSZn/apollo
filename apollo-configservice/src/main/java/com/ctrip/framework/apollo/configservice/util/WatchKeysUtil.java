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
package com.ctrip.framework.apollo.configservice.util;

import static com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator.generate;

import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class WatchKeysUtil {
  private final AppNamespaceServiceWithCache appNamespaceService;

  public WatchKeysUtil(final AppNamespaceServiceWithCache appNamespaceService) {
    this.appNamespaceService = appNamespaceService;
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespace, dataCenter combination
   */
  public Set<String> assembleAllWatchKeys(String appId, String clusterName, String namespace,
                                          String dataCenter) {
    Multimap<String, String> watchedKeysMap =
        assembleAllWatchKeys(appId, clusterName, Sets.newHashSet(namespace), dataCenter);
    return Sets.newHashSet(watchedKeysMap.get(namespace));
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespaces, dataCenter combination
   *
   * @return a multimap with namespace as the key and watch keys as the value
   */
  public Multimap<String, String> assembleAllWatchKeys(String appId, String clusterName,
                                                       Set<String> namespaces,
                                                       String dataCenter) {
    Multimap<String, String> watchedKeysMap =
        assembleWatchKeys(appId, clusterName, namespaces, dataCenter);

    //Every app has an 'application' namespace
    if (!(namespaces.size() == 1 && namespaces.contains(ConfigConsts.NAMESPACE_APPLICATION))) {
      Set<String> namespacesBelongToAppId = namespacesBelongToAppId(appId, namespaces);
      // 筛查出 公共命名空间的名称
      Set<String> publicNamespaces = Sets.difference(namespaces, namespacesBelongToAppId);

      // 补充公共命名空间的watchKey
      //Listen on more namespaces if it's a public namespace
      if (!publicNamespaces.isEmpty()) {
        watchedKeysMap
            .putAll(findPublicConfigWatchKeys(appId, clusterName, publicNamespaces, dataCenter));
      }
    }

    return watchedKeysMap;
  }

  /**
   * 组装公共命名空间的watchKey
   * @param applicationId   应用ID
   * @param clusterName     集群名称
   * @param namespaces      命名空间名称集合
   * @param dataCenter      数据中心
   * @return
   */
  private Multimap<String, String> findPublicConfigWatchKeys(String applicationId,
                                                             String clusterName,
                                                             Set<String> namespaces,
                                                             String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();
    List<AppNamespace> appNamespaces = appNamespaceService.findPublicNamespacesByNames(namespaces);

    for (AppNamespace appNamespace : appNamespaces) {
      //check whether the namespace's appId equals to current one
      if (Objects.equals(applicationId, appNamespace.getAppId())) {
        continue;
      }

      String publicConfigAppId = appNamespace.getAppId();

      watchedKeysMap.putAll(appNamespace.getName(),
          assembleWatchKeys(publicConfigAppId, clusterName, appNamespace.getName(), dataCenter));
    }

    return watchedKeysMap;
  }

  private Set<String> assembleWatchKeys(String appId, String clusterName, String namespace,
                                        String dataCenter) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    Set<String> watchedKeys = Sets.newHashSet();

    // 如果不是默认集群，则会添加指定集群的指定Namespace来监听
    //watch specified cluster config change
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, clusterName)) {
      watchedKeys.add(generate(appId, clusterName, namespace));
    }

    // 如果默认数据中心不为空，则还会添加指定集群的指定Namespace来监听
    //watch data center config change
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, clusterName)) {
      watchedKeys.add(generate(appId, dataCenter, namespace));
    }

    // 监听默认集群的Namespace
    //watch default cluster config change
    watchedKeys.add(generate(appId, ConfigConsts.CLUSTER_NAME_DEFAULT, namespace));

    return watchedKeys;
  }

  /**
   * 组装需要监听的key。每一个key有 appID.clusterName.namespaceName 组成
   * @param appId
   * @param clusterName
   * @param namespaces
   * @param dataCenter
   * @return
   */
  private Multimap<String, String> assembleWatchKeys(String appId, String clusterName,
                                                     Set<String> namespaces,
                                                     String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();

    for (String namespace : namespaces) {
      watchedKeysMap
          .putAll(namespace, assembleWatchKeys(appId, clusterName, namespace, dataCenter));
    }

    return watchedKeysMap;
  }

  /**
   * 查询传入的 namespaces 中所有属于指定 appID 所属应用的 namespace的名称
   * @param appId
   * @param namespaces
   * @return
   */
  private Set<String> namespacesBelongToAppId(String appId, Set<String> namespaces) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    List<AppNamespace> appNamespaces =
        appNamespaceService.findByAppIdAndNamespaces(appId, namespaces);

    if (appNamespaces == null || appNamespaces.isEmpty()) {
      return Collections.emptySet();
    }

    return appNamespaces.stream().map(AppNamespace::getName).collect(Collectors.toSet());
  }
}
