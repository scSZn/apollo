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
package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.repository.AppNamespaceRepository;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.wrapper.CaseInsensitiveMapWrapper;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>带缓存的AppNamespace服务</p>
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class AppNamespaceServiceWithCache implements InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(AppNamespaceServiceWithCache.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR)
      .skipNulls();
  private final AppNamespaceRepository appNamespaceRepository;
  private final BizConfig bizConfig;

  private int scanInterval;
  private TimeUnit scanIntervalTimeUnit;
  private int rebuildInterval;
  private TimeUnit rebuildIntervalTimeUnit;
  private ScheduledExecutorService scheduledExecutorService;
  private long maxIdScanned;

  //store namespaceName -> AppNamespace
  private CaseInsensitiveMapWrapper<AppNamespace> publicAppNamespaceCache;

  //store appId+namespaceName -> AppNamespace
  private CaseInsensitiveMapWrapper<AppNamespace> appNamespaceCache;

  //store id -> AppNamespace
  private Map<Long, AppNamespace> appNamespaceIdCache;

  public AppNamespaceServiceWithCache(
      final AppNamespaceRepository appNamespaceRepository,
      final BizConfig bizConfig) {
    this.appNamespaceRepository = appNamespaceRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    maxIdScanned = 0;
    publicAppNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceCache = new CaseInsensitiveMapWrapper<>(Maps.newConcurrentMap());
    appNamespaceIdCache = Maps.newConcurrentMap();
    scheduledExecutorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("AppNamespaceServiceWithCache", true));
  }

  public AppNamespace findByAppIdAndNamespace(String appId, String namespaceName) {
    Preconditions.checkArgument(!StringUtils.isContainEmpty(appId, namespaceName), "appId and namespaceName must not be empty");
    return appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
  }

  public List<AppNamespace> findByAppIdAndNamespaces(String appId, Set<String> namespaceNames) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appId), "appId must not be null");
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = appNamespaceCache.get(STRING_JOINER.join(appId, namespaceName));
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  public AppNamespace findPublicNamespaceByName(String namespaceName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespaceName), "namespaceName must not be empty");
    return publicAppNamespaceCache.get(namespaceName);
  }

  public List<AppNamespace> findPublicNamespacesByNames(Set<String> namespaceNames) {
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }

    List<AppNamespace> result = Lists.newArrayList();
    for (String namespaceName : namespaceNames) {
      AppNamespace appNamespace = publicAppNamespaceCache.get(namespaceName);
      if (appNamespace != null) {
        result.add(appNamespace);
      }
    }
    return result;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    // 1. 填充数据库配置
    populateDataBaseInterval();
    // 2. 启动的时候先扫描一次
    scanNewAppNamespaces(); //block the startup process until load finished
    // 3. 启动定时任务，定时重建缓存，这里重建是完全重建
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
          "rebuildCache");
      try {
        this.updateAndDeleteCache();
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        transaction.setStatus(ex);
        logger.error("Rebuild cache failed", ex);
      } finally {
        transaction.complete();
      }
    }, rebuildInterval, rebuildInterval, rebuildIntervalTimeUnit);
    // 4. 启动定时任务，定时将新增的AppNamespace添加到缓存中
    scheduledExecutorService.scheduleWithFixedDelay(this::scanNewAppNamespaces, scanInterval,
        scanInterval, scanIntervalTimeUnit);
  }

  /**
   * 扫描新的 AppNamespace。注意是新的，这里只扫描新增加的AppNamespace，如果是删除的话，这个函数不会处理
   */
  private void scanNewAppNamespaces() {
    Transaction transaction = Tracer.newTransaction("Apollo.AppNamespaceServiceWithCache",
        "scanNewAppNamespaces");
    try {
      // 加载AppNamespace
      this.loadNewAppNamespaces();
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Load new app namespaces failed", ex);
    } finally {
      transaction.complete();
    }
  }

  /**
   * 加载AppNamespace。批量扫描数据库中 AppNamespace表，将其全量缓存到本缓存中
   */
  //for those new app namespaces
  private void loadNewAppNamespaces() {
    boolean hasMore = true;
    // 每次扫描500条AppNamespace入缓存
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500
      List<AppNamespace> appNamespaces = appNamespaceRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
      if (CollectionUtils.isEmpty(appNamespaces)) {
        break;
      }
      // 将查询到的AppNamespaces添加到当前缓存中
      mergeAppNamespaces(appNamespaces);
      int scanned = appNamespaces.size();
      // 更新当前扫描的最大ID值
      maxIdScanned = appNamespaces.get(scanned - 1).getId();
      hasMore = scanned == 500;
      logger.info("Loaded {} new app namespaces with startId {}", scanned, maxIdScanned);
    }
  }

  /**
   * 将这些AppNamespace填充到当前缓存中
   * @param appNamespaces
   */
  private void mergeAppNamespaces(List<AppNamespace> appNamespaces) {
    for (AppNamespace appNamespace : appNamespaces) {
      appNamespaceCache.put(assembleAppNamespaceKey(appNamespace), appNamespace);
      appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
      if (appNamespace.isPublic()) {
        publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);
      }
    }
  }

  /**
   * 这里是完全重建AppNamespace缓存的逻辑
   */
  //for those updated or deleted app namespaces
  private void updateAndDeleteCache() {
    // 1. 这里先获取到当前缓存的所有AppNamespace的ID
    List<Long> ids = Lists.newArrayList(appNamespaceIdCache.keySet());
    if (CollectionUtils.isEmpty(ids)) {
      return;
    }
    // 2. 这里回去数据库中查询这些ID的数据，进行删除和更新
    List<List<Long>> partitionIds = Lists.partition(ids, 500);
    for (List<Long> toRebuild : partitionIds) {
      Iterable<AppNamespace> appNamespaces = appNamespaceRepository.findAllById(toRebuild);

      if (appNamespaces == null) {
        continue;
      }

      // 2.1 进行更新
      //handle updated
      Set<Long> foundIds = handleUpdatedAppNamespaces(appNamespaces);

      // 2.2 进行删除
      //handle deleted
      handleDeletedAppNamespaces(Sets.difference(Sets.newHashSet(toRebuild), foundIds));
    }
  }

  /**
   * 更新 AppNamespace 缓存
   * @param appNamespaces
   * @return 返回数据库中有的AppNamespace的ID，后续会将那些被删除的AppNamespace（即数据库中不存在的AppNamespace）给
   */
  //for those updated app namespaces
  private Set<Long> handleUpdatedAppNamespaces(Iterable<AppNamespace> appNamespaces) {
    Set<Long> foundIds = Sets.newHashSet();
    for (AppNamespace appNamespace : appNamespaces) {
      foundIds.add(appNamespace.getId());
      // 1. 取出在缓存中的AppNamespace
      AppNamespace thatInCache = appNamespaceIdCache.get(appNamespace.getId());
      // 判断是否需要进行更新。如果thatInCache为空，说明是新增，不管，新增的处理逻辑在 scanNewAppNamespaced处理
      if (thatInCache != null && appNamespace.getDataChangeLastModifiedTime().after(thatInCache
          .getDataChangeLastModifiedTime())) {
        // 2. 更新缓存
        appNamespaceIdCache.put(appNamespace.getId(), appNamespace);
        String oldKey = assembleAppNamespaceKey(thatInCache);
        String newKey = assembleAppNamespaceKey(appNamespace);
        appNamespaceCache.put(newKey, appNamespace);

        // 3. 如果 appId 或者 namespace的名称发生了变化，但是ID没有变化，还需要清除缓存
        //in case appId or namespaceName changes
        if (!newKey.equals(oldKey)) {
          appNamespaceCache.remove(oldKey);
        }

        // 4. 如果是public类型的AppNamespace，还需要清理public相关的缓存
        if (appNamespace.isPublic()) {
          publicAppNamespaceCache.put(appNamespace.getName(), appNamespace);

          //in case namespaceName changes
          if (!appNamespace.getName().equals(thatInCache.getName()) && thatInCache.isPublic()) {
            publicAppNamespaceCache.remove(thatInCache.getName());
          }
        } else if (thatInCache.isPublic()) {
          // 这里是防止Namespace从public变成了private
          //just in case isPublic changes
          publicAppNamespaceCache.remove(thatInCache.getName());
        }
        logger.info("Found AppNamespace changes, old: {}, new: {}", thatInCache, appNamespace);
      }
    }
    return foundIds;
  }

  /**
   * 处理那些需要删除的 AppNamespace，没啥特殊的，就是删除一遍
   * @param deletedIds
   */
  //for those deleted app namespaces
  private void handleDeletedAppNamespaces(Set<Long> deletedIds) {
    if (CollectionUtils.isEmpty(deletedIds)) {
      return;
    }
    for (Long deletedId : deletedIds) {
      AppNamespace deleted = appNamespaceIdCache.remove(deletedId);
      if (deleted == null) {
        continue;
      }
      appNamespaceCache.remove(assembleAppNamespaceKey(deleted));
      if (deleted.isPublic()) {
        AppNamespace publicAppNamespace = publicAppNamespaceCache.get(deleted.getName());
        // in case there is some dirty data, e.g. public namespace deleted in some app and now created in another app
        if (publicAppNamespace == deleted) {
          publicAppNamespaceCache.remove(deleted.getName());
        }
      }
      logger.info("Found AppNamespace deleted, {}", deleted);
    }
  }

  private String assembleAppNamespaceKey(AppNamespace appNamespace) {
    return STRING_JOINER.join(appNamespace.getAppId(), appNamespace.getName());
  }

  /**
   * 填充数据库相关配置属性，包括 定时扫描数据库中 AppNamespace 数据表的间隔以及重建缓存的间隔时长
   */
  private void populateDataBaseInterval() {
    scanInterval = bizConfig.appNamespaceCacheScanInterval();
    scanIntervalTimeUnit = bizConfig.appNamespaceCacheScanIntervalTimeUnit();
    rebuildInterval = bizConfig.appNamespaceCacheRebuildInterval();
    rebuildIntervalTimeUnit = bizConfig.appNamespaceCacheRebuildIntervalTimeUnit();
  }

  //only for test use
  private void reset() throws Exception {
    scheduledExecutorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
