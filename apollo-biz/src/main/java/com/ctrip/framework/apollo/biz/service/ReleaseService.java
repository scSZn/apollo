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

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseHistory;
import com.ctrip.framework.apollo.biz.repository.ReleaseRepository;
import com.ctrip.framework.apollo.biz.utils.ReleaseKeyGenerator;
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang.time.FastDateFormat;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseService {

  private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");
  private static final Gson GSON = new Gson();
  private static final Set<Integer> BRANCH_RELEASE_OPERATIONS = Sets
      .newHashSet(ReleaseOperation.GRAY_RELEASE, ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY);
  private static final Pageable FIRST_ITEM = PageRequest.of(0, 1);
  private static final Type OPERATION_CONTEXT_TYPE_REFERENCE = new TypeToken<Map<String, Object>>() { }.getType();

  private final ReleaseRepository releaseRepository;
  private final ItemService itemService;
  private final AuditService auditService;
  private final NamespaceLockService namespaceLockService;
  private final NamespaceService namespaceService;
  private final NamespaceBranchService namespaceBranchService;
  private final ReleaseHistoryService releaseHistoryService;
  private final ItemSetService itemSetService;

  public ReleaseService(
      final ReleaseRepository releaseRepository,
      final ItemService itemService,
      final AuditService auditService,
      final NamespaceLockService namespaceLockService,
      final NamespaceService namespaceService,
      final NamespaceBranchService namespaceBranchService,
      final ReleaseHistoryService releaseHistoryService,
      final ItemSetService itemSetService) {
    this.releaseRepository = releaseRepository;
    this.itemService = itemService;
    this.auditService = auditService;
    this.namespaceLockService = namespaceLockService;
    this.namespaceService = namespaceService;
    this.namespaceBranchService = namespaceBranchService;
    this.releaseHistoryService = releaseHistoryService;
    this.itemSetService = itemSetService;
  }

  public Release findOne(long releaseId) {
    return releaseRepository.findById(releaseId).orElse(null);
  }


  public Release findActiveOne(long releaseId) {
    return releaseRepository.findByIdAndIsAbandonedFalse(releaseId);
  }

  public List<Release> findByReleaseIds(Set<Long> releaseIds) {
    Iterable<Release> releases = releaseRepository.findAllById(releaseIds);
    if (releases == null) {
      return Collections.emptyList();
    }
    return Lists.newArrayList(releases);
  }

  public List<Release> findByReleaseKeys(Set<String> releaseKeys) {
    return releaseRepository.findByReleaseKeyIn(releaseKeys);
  }

  /**
   * 找到最近一次有效的发布
   * @param namespace
   * @return
   */
  public Release findLatestActiveRelease(Namespace namespace) {
    return findLatestActiveRelease(namespace.getAppId(),
                                   namespace.getClusterName(), namespace.getNamespaceName());

  }

  /**
   * 找到指定Namespace的最近发布记录，如果发布已经放弃了，则不计入此列
   * @param appId
   * @param clusterName
   * @param namespaceName
   * @return
   */
  public Release findLatestActiveRelease(String appId, String clusterName, String namespaceName) {
    return releaseRepository.findFirstByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId,
                                                                                                            clusterName,
                                                                                                            namespaceName);
  }

  public List<Release> findAllReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release> releases = releaseRepository.findByAppIdAndClusterNameAndNamespaceNameOrderByIdDesc(appId,
                                                                                                      clusterName,
                                                                                                      namespaceName,
                                                                                                      page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  /**
   * 找到Namespace最近的几次发布记录，且是未被废弃的发布记录
   * @param appId
   * @param clusterName
   * @param namespaceName
   * @param page
   * @return
   */
  public List<Release> findActiveReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId, clusterName,
                                                                                                    namespaceName,
                                                                                                    page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  private List<Release> findActiveReleasesBetween(String appId, String clusterName, String namespaceName,
                                                  long fromReleaseId, long toReleaseId) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseAndIdBetweenOrderByIdDesc(appId,
                                                                                                                clusterName,
                                                                                                                namespaceName,
                                                                                                                fromReleaseId,
                                                                                                                toReleaseId);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  @Transactional
  public Release mergeBranchChangeSetsAndRelease(Namespace namespace, String branchName, String releaseName,
                                                 String releaseComment, boolean isEmergencyPublish,
                                                 ItemChangeSets changeSets) {

    checkLock(namespace, isEmergencyPublish, changeSets.getDataChangeLastModifiedBy());

    itemSetService.updateSet(namespace, changeSets);

    Release branchRelease = findLatestActiveRelease(namespace.getAppId(), branchName, namespace
        .getNamespaceName());
    long branchReleaseId = branchRelease == null ? 0 : branchRelease.getId();

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Map<String, Object> operationContext = Maps.newLinkedHashMap();
    operationContext.put(ReleaseOperationContext.SOURCE_BRANCH, branchName);
    operationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, branchReleaseId);
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    return masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                         changeSets.getDataChangeLastModifiedBy(),
                         ReleaseOperation.GRAY_RELEASE_MERGE_TO_MASTER, operationContext);

  }

  /**
   * 发布Namespace
   * @param namespace
   * @param releaseName
   * @param releaseComment
   * @param operator
   * @param isEmergencyPublish
   * @return
   */
  @Transactional
  public Release publish(Namespace namespace, String releaseName, String releaseComment,
                         String operator, boolean isEmergencyPublish) {
    // 1. 检查锁，如果配置中开启了一次发布只能有一个人修改，那么在修改的人发布之前，其他人是不能做修改的
    // 同时，也不能由修改的这个人进行发布
    checkLock(namespace, isEmergencyPublish, operator);
    // 2. 获取到Namespace的配置项
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);
    // 3. 查询该Namespace的父级Namespace，主要是为了判断需要发布的Namespace是否是灰度版本
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    // 4. 发布分支
    //branch release
    if (parentNamespace != null) {
      // 4.1 如果是灰度版本的Namespace，则说明是灰度发布，走灰度发布流程
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
                                    releaseName, releaseComment, operator, isEmergencyPublish);
    }

    // 4.2 如果不是灰度版本的Namespace，则说明是全量发布，走全量发布的流程
    // 4.2.1 找到指定Namespace的灰度版本
    Namespace childNamespace = namespaceService.findChildNamespace(namespace);

    // 4.2.2 查询最近的一次发布记录
    Release previousRelease = null;
    if (childNamespace != null) {
      previousRelease = findLatestActiveRelease(namespace);
    }

    // 4.2.3 构造发布上下文
    //master release
    Map<String, Object> operationContext = Maps.newLinkedHashMap();
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    // 4.2.4 创建Release实体
    Release release = masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                                    operator, ReleaseOperation.NORMAL_RELEASE, operationContext);

    // 4.2.5 如果有灰度版本，这里还需要把主版本的配置，合并到灰度版本进行发布
    //merge to branch and auto release
    if (childNamespace != null) {
      mergeFromMasterAndPublishBranch(namespace, childNamespace, operateNamespaceItems,
                                      releaseName, releaseComment, operator, previousRelease,
                                      release, isEmergencyPublish);
    }

    return release;
  }

  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {
    Release parentLatestRelease = findLatestActiveRelease(parentNamespace);
    Map<String, String> parentConfigurations = parentLatestRelease != null ?
            GSON.fromJson(parentLatestRelease.getConfigurations(),
                          GsonType.CONFIG) : new LinkedHashMap<>();
    long baseReleaseId = parentLatestRelease == null ? 0 : parentLatestRelease.getId();

    Map<String, String> configsToPublish = mergeConfiguration(parentConfigurations, childNamespaceItems);

    if(!(grayDelKeys == null || grayDelKeys.size()==0)){
      for (String key : grayDelKeys){
        configsToPublish.remove(key);
      }
    }

    return branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
        configsToPublish, baseReleaseId, operator, ReleaseOperation.GRAY_RELEASE, isEmergencyPublish,
        childNamespaceItems.keySet());

  }

  @Transactional
  public Release grayDeletionPublish(Namespace namespace, String releaseName, String releaseComment,
                                     String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {

    checkLock(namespace, isEmergencyPublish, operator);

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
              releaseName, releaseComment, operator, isEmergencyPublish, grayDelKeys);
    }
    throw new NotFoundException("Parent namespace not found");
  }

  /**
   * 如果不是紧急发布，需要校验一下这个锁
   * @param namespace
   * @param isEmergencyPublish
   * @param operator
   */
  private void checkLock(Namespace namespace, boolean isEmergencyPublish, String operator) {
    if (!isEmergencyPublish) {
      NamespaceLock lock = namespaceLockService.findLock(namespace.getId());
      if (lock != null && lock.getDataChangeCreatedBy().equals(operator)) {
        throw new BadRequestException("Config can not be published by yourself.");
      }
    }
  }

  /**
   * 把Namespace主版本的配置合并到其灰度版本，然后进行一次灰度发布
   * <p>
   *     这个功能简单的说，就是将主版本新的配置，合并到已有的灰度版本中，进行一次灰度发布，使得那些在主版本中新增或者修改，但灰度版本中没有的配置项，也能在灰度版本中生效
   * </p>
   * @param parentNamespace         主版本的Namespace
   * @param childNamespace          灰度版本的Namespace
   * @param parentNamespaceItems    主版本Namespace的配置项
   * @param releaseName             本次发布的主题
   * @param releaseComment          本次发布的备注
   * @param operator                操作人
   * @param masterPreviousRelease   主版本之前的那个发布记录
   * @param parentRelease           主版本当前的发布记录
   * @param isEmergencyPublish      是否紧急发布
   */
  private void mergeFromMasterAndPublishBranch(Namespace parentNamespace, Namespace childNamespace,
                                               Map<String, String> parentNamespaceItems,
                                               String releaseName, String releaseComment,
                                               String operator, Release masterPreviousRelease,
                                               Release parentRelease, boolean isEmergencyPublish) {
    // 1. 先找到当前灰度版本的最近一次发布记录
    //create release for child namespace
    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);

    Map<String, String> childReleaseConfiguration;    // 灰度版本中正在发布的配置
    Collection<String> branchReleaseKeys;             // 灰度版本中发布的配置项
    if (childNamespaceLatestActiveRelease != null) {
      // 解析到上一次灰度发布的配置项，和涉及到的key
      childReleaseConfiguration = GSON.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    // 2. 解析出主版本的Namespace的最近一次发布中的配置信息
    Map<String, String> parentNamespaceOldConfiguration = masterPreviousRelease == null ?
                                                          null : GSON.fromJson(masterPreviousRelease.getConfigurations(),
                                                                               GsonType.CONFIG);
    // 3. 合并这三个配置项
    // 主要需要的就是将当前主版本需要发布的配置，和已经发布的灰度版本的配置做一个合并
    // 因为灰度发布实际是以灰度的key作为基础的，因此只需要以主版本的配置为基础，同时用灰度版本涉及到的key覆盖主版本配置即可
    Map<String, String> childNamespaceToPublishConfigs =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceOldConfiguration, parentNamespaceItems,
            childReleaseConfiguration, branchReleaseKeys);

    //compare
    // 4. 如果合并后的灰度发布配置，与之前的灰度发布配置有差别，则进行一次发布。发布类型为，主版本合并到灰度版本后的发布
    if (!childNamespaceToPublishConfigs.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
                    childNamespaceToPublishConfigs, parentRelease.getId(), operator,
                    ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY, isEmergencyPublish, branchReleaseKeys);
    }

  }

  /**
   * 获取到灰度发布涉及到的key
   * @param releaseId
   * @return
   */
  private Collection<String> getBranchReleaseKeys(long releaseId) {
    // 1. 根据releaseId和发布操作类型获取到最近的一次发布历史记录
    Page<ReleaseHistory> releaseHistories = releaseHistoryService
        .findByReleaseIdAndOperationInOrderByIdDesc(releaseId, BRANCH_RELEASE_OPERATIONS, FIRST_ITEM);

    // 2. 如果没有发布历史记录，则直接返回孔
    if (!releaseHistories.hasContent()) {
      return null;
    }

    // 3. 获取发布历史记录的发布上下文信息
    String operationContextJson = releaseHistories.getContent().get(0).getOperationContext();
    if (Strings.isNullOrEmpty(operationContextJson)
        || !operationContextJson.contains(ReleaseOperationContext.BRANCH_RELEASE_KEYS)) {
      return null;
    }

    // 4. 解析出发布上下文
    Map<String, Object> operationContext = GSON.fromJson(operationContextJson,
        OPERATION_CONTEXT_TYPE_REFERENCE);

    if (operationContext == null) {
      return null;
    }

    // 5. 返回发布上下文中的发布Key信息
    return (Collection<String>) operationContext.get(ReleaseOperationContext.BRANCH_RELEASE_KEYS);
  }

  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish) {
    return publishBranchNamespace(parentNamespace, childNamespace, childNamespaceItems, releaseName, releaseComment,
            operator, isEmergencyPublish, null);

  }

  /**
   * 创建主版本发布Release的实体
   * @param namespace         命名空间
   * @param releaseName       本次发布的主题名称
   * @param releaseComment    本次发布的备注
   * @param configurations    配置集合
   * @param operator          操作人
   * @param releaseOperation  发布操作的类型，见 {@link com.ctrip.framework.apollo.common.constants.ReleaseOperation}
   * @param operationContext  发布上下文
   * @return
   */
  private Release masterRelease(Namespace namespace, String releaseName, String releaseComment,
                                Map<String, String> configurations, String operator,
                                int releaseOperation, Map<String, Object> operationContext) {
    // 1. 首先找到最近一次有效的发布
    Release lastActiveRelease = findLatestActiveRelease(namespace);
    long previousReleaseId = lastActiveRelease == null ? 0 : lastActiveRelease.getId();
    // 2. 创建Release实体
    Release release = createRelease(namespace, releaseName, releaseComment,
                                    configurations, operator);
    // 3. 创建发布历史
    releaseHistoryService.createReleaseHistory(namespace.getAppId(), namespace.getClusterName(),
                                               namespace.getNamespaceName(), namespace.getClusterName(),
                                               release.getId(), previousReleaseId, releaseOperation,
                                               operationContext, operator);

    return release;
  }

  /**
   * 触发一次灰度版本的Namespace的发布
   * @param parentNamespace     主版本的Namespace
   * @param childNamespace      灰度版本的Namespace
   * @param releaseName         发布名称
   * @param releaseComment      发布备注
   * @param configurations      需要发布的配置项
   * @param baseReleaseId       基础发布ID。如果当前灰度发布是因为主版本的配置变更后发布而导致需要一次灰度发布，则这里就是主版本的发布ID
   * @param operator            操作人
   * @param releaseOperation    发布类型，见{@link com.ctrip.framework.apollo.common.constants.ReleaseOperation}
   * @param isEmergencyPublish  是否紧急发布
   * @param branchReleaseKeys   灰度发布中，灰度的key是哪些
   * @return
   */
  private Release branchRelease(Namespace parentNamespace, Namespace childNamespace,
                                String releaseName, String releaseComment,
                                Map<String, String> configurations, long baseReleaseId,
                                String operator, int releaseOperation, boolean isEmergencyPublish, Collection<String> branchReleaseKeys) {
    // 1. 先找到灰度发布最近的一次发布记录
    Release previousRelease = findLatestActiveRelease(childNamespace.getAppId(),
                                                      childNamespace.getClusterName(),
                                                      childNamespace.getNamespaceName());
    long previousReleaseId = previousRelease == null ? 0 : previousRelease.getId();

    // 2. 构造发布上下文
    Map<String, Object> releaseOperationContext = Maps.newLinkedHashMap();
    releaseOperationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, baseReleaseId);
    releaseOperationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);
    releaseOperationContext.put(ReleaseOperationContext.BRANCH_RELEASE_KEYS, branchReleaseKeys);

    // 3. 创建新的发布记录
    Release release =
        createRelease(childNamespace, releaseName, releaseComment, configurations, operator);

    //update gray release rules
    GrayReleaseRule grayReleaseRule = namespaceBranchService.updateRulesReleaseId(childNamespace.getAppId(),
                                                                                  parentNamespace.getClusterName(),
                                                                                  childNamespace.getNamespaceName(),
                                                                                  childNamespace.getClusterName(),
                                                                                  release.getId(), operator);

    if (grayReleaseRule != null) {
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(grayReleaseRule.getRules()));
    }

    releaseHistoryService.createReleaseHistory(parentNamespace.getAppId(), parentNamespace.getClusterName(),
                                               parentNamespace.getNamespaceName(), childNamespace.getClusterName(),
                                               release.getId(),
                                               previousReleaseId, releaseOperation, releaseOperationContext, operator);

    return release;
  }

  /**
   * 合并两个配置项，以baseConfigurations，将coverConfigurations合并上去
   * <li>
   *     如果有相同的Key，则用coverConfigurations中的值覆盖baseConfigurations的值
   * </li>
   * <li>
   *     如果有新增的Key，则补充上去
   * </li>
   * @param baseConfigurations
   * @param coverConfigurations
   * @return
   */
  private Map<String, String> mergeConfiguration(Map<String, String> baseConfigurations,
                                                 Map<String, String> coverConfigurations) {
    int expectedSize = baseConfigurations.size() + coverConfigurations.size();
    Map<String, String> result = Maps.newLinkedHashMapWithExpectedSize(expectedSize);

    //copy base configuration
    result.putAll(baseConfigurations);

    //update and publish
    result.putAll(coverConfigurations);

    return result;
  }

  /**
   * 获取指定Namespace下的配置项，返回一个Map类型，key为配置项的key，value为配置项的值
   * @param namespace
   * @return
   */
  private Map<String, String> getNamespaceItems(Namespace namespace) {
    List<Item> items = itemService.findItemsWithOrdered(namespace.getId());
    Map<String, String> configurations = new LinkedHashMap<>();
    for (Item item : items) {
      if (StringUtils.isEmpty(item.getKey())) {
        continue;
      }
      configurations.put(item.getKey(), item.getValue());
    }

    return configurations;
  }

  private Release createRelease(Namespace namespace, String name, String comment,
                                Map<String, String> configurations, String operator) {
    Release release = new Release();
    release.setReleaseKey(ReleaseKeyGenerator.generateReleaseKey(namespace));
    release.setDataChangeCreatedTime(new Date());
    release.setDataChangeCreatedBy(operator);
    release.setDataChangeLastModifiedBy(operator);
    release.setName(name);
    release.setComment(comment);
    release.setAppId(namespace.getAppId());
    release.setClusterName(namespace.getClusterName());
    release.setNamespaceName(namespace.getNamespaceName());
    release.setConfigurations(GSON.toJson(configurations));
    release = releaseRepository.save(release);

    // 发布之后直接解锁
    namespaceLockService.unlock(namespace.getId());
    auditService.audit(Release.class.getSimpleName(), release.getId(), Audit.OP.INSERT,
                       release.getDataChangeCreatedBy());

    return release;
  }

  @Transactional
  public Release rollback(long releaseId, String operator) {
    // 1. 先找到当前发布记录，需要判断当前发布记录是否是被废弃的
    Release release = findOne(releaseId);
    if (release == null) {
      throw NotFoundException.releaseNotFound(releaseId);
    }
    if (release.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    // 2. 找到最近的2次发布记录。理论上来说，最近的一次发布记录应该就是release
    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    PageRequest page = PageRequest.of(0, 2);
    List<Release> twoLatestActiveReleases = findActiveReleases(appId, clusterName, namespaceName, page);
    if (twoLatestActiveReleases == null || twoLatestActiveReleases.size() < 2) {
      throw new BadRequestException(
          "Can't rollback namespace(appId=%s, clusterName=%s, namespaceName=%s) because there is only one active release",
          appId,
          clusterName,
          namespaceName);
    }

    // 3. 将当前生效的发布记录置为废弃
    release.setAbandoned(true);
    release.setDataChangeLastModifiedBy(operator);

    releaseRepository.save(release);

    // 4. 创建发布历史
    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, twoLatestActiveReleases.get(1).getId(),
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    // 5. 如果当前Namespace有灰度版本，还需要合并配置，重新发布灰度版本
    rollbackChildNamespace(appId, clusterName, namespaceName, twoLatestActiveReleases, operator);

    return release;
  }

  /**
   * 回滚到指定版本的Namespace
   * @param releaseId       回滚前的版本
   * @param toReleaseId     回滚后的版本
   * @param operator        操作人
   * @return
   */
  @Transactional
  public Release rollbackTo(long releaseId, long toReleaseId, String operator) {
    if (releaseId == toReleaseId) {
      throw new BadRequestException("current release equal to target release");
    }

    // 1. 找到两个发布记录
    Release release = findOne(releaseId);
    Release toRelease = findOne(toReleaseId);

    if (release == null) {
      throw NotFoundException.releaseNotFound(releaseId);
    }
    if (toRelease == null) {
      throw NotFoundException.releaseNotFound(toReleaseId);
    }
    if (release.isAbandoned() || toRelease.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    // 2. 将这两个发布记录之间的所有有效版本均置为废弃
    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    List<Release> releases = findActiveReleasesBetween(appId, clusterName, namespaceName,
                                                       toReleaseId, releaseId);

    for (int i = 0; i < releases.size() - 1; i++) {
      releases.get(i).setAbandoned(true);
      releases.get(i).setDataChangeLastModifiedBy(operator);
    }

    releaseRepository.saveAll(releases);

    // 3. 创建发布历史
    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, toReleaseId,
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    // 4. 如果有灰度版本，还需要将现在的配置合并到灰度版本后触发一次灰度发布
    rollbackChildNamespace(appId, clusterName, namespaceName, Lists.newArrayList(release, toRelease), operator);

    return release;
  }

  /**
   * 回滚指定Namespace的灰度版本的配置
   * <p>
   *     简单说，就是将 parentNamespaceTwoLatestActiveRelease[1] 中发布的配置与灰度版本做一个合并，然后重新发布灰度
   * </p>
   * @param appId             应用ID
   * @param clusterName       集群名称
   * @param namespaceName     命名空间名称
   * @param parentNamespaceTwoLatestActiveRelease 主版本Namespace的最近两次发布记录
   * @param operator          操作人
   */
  private void rollbackChildNamespace(String appId, String clusterName, String namespaceName,
                                      List<Release> parentNamespaceTwoLatestActiveRelease, String operator) {
    // 1. 找到主版本和灰度版本的Namespace
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    Namespace childNamespace = namespaceService.findChildNamespace(appId, clusterName, namespaceName);
    if (parentNamespace == null || childNamespace == null) {
      return;
    }
    // 2. 找到灰度版本最近的一次发布记录，解析配置，然后合并配置
    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);
    Map<String, String> childReleaseConfiguration;
    Collection<String> branchReleaseKeys;
    if (childNamespaceLatestActiveRelease != null) {
      childReleaseConfiguration = GSON.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    Release abandonedRelease = parentNamespaceTwoLatestActiveRelease.get(0);
    Release parentNamespaceNewLatestRelease = parentNamespaceTwoLatestActiveRelease.get(1);

    // 回滚前，主版本的Namespace的配置项
    Map<String, String> parentNamespaceAbandonedConfiguration = GSON.fromJson(abandonedRelease.getConfigurations(),
                                                                              GsonType.CONFIG);
    // 回滚后，主版本的Namespace的配置项
    Map<String, String>
        parentNamespaceNewLatestConfiguration =
        GSON.fromJson(parentNamespaceNewLatestRelease.getConfigurations(), GsonType.CONFIG);

    // 重新计算出灰度版本应该有的配置项
    Map<String, String>
        childNamespaceNewConfiguration =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceAbandonedConfiguration,
            parentNamespaceNewLatestConfiguration, childReleaseConfiguration, branchReleaseKeys);

    //compare
    // 3. 触发分支发布
    if (!childNamespaceNewConfiguration.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace,
          TIMESTAMP_FORMAT.format(new Date()) + "-master-rollback-merge-to-gray", "",
          childNamespaceNewConfiguration, parentNamespaceNewLatestRelease.getId(), operator,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY, false, branchReleaseKeys);
    }
  }

  /**
   * 计算出需要发布的灰度版本的配置项
   * @param parentNamespaceOldConfiguration           主版本的最近一次发布的配置项。即主版本老的配置项
   * @param parentNamespaceNewConfiguration           主版本现在需要发布的配置项。即主版本新的配置项
   * @param childNamespaceLatestActiveConfiguration   灰度版本最近一次发布的配置项。即灰度版本老的配置项
   * @param branchReleaseKeys                         灰度版本最近一次发布涉及到的key
   * @return
   */
  private Map<String, String> calculateChildNamespaceToPublishConfiguration(
      Map<String, String> parentNamespaceOldConfiguration, Map<String, String> parentNamespaceNewConfiguration,
      Map<String, String> childNamespaceLatestActiveConfiguration, Collection<String> branchReleaseKeys) {
    //first. calculate child namespace modified configs
    // 1. 从主版本的Namespace的最近发布配置，和灰度版本的最近发布配置中，找到不同的配置项
    Map<String, String> childNamespaceModifiedConfiguration = calculateBranchModifiedItemsAccordingToRelease(
        parentNamespaceOldConfiguration, childNamespaceLatestActiveConfiguration, branchReleaseKeys);

    //second. append child namespace modified configs to parent namespace new latest configuration
    // 2. 合并两个版本的配置，用childNamespaceModifiedConfiguration的配置，添加到parentNamespaceNewConfiguration中
    return mergeConfiguration(parentNamespaceNewConfiguration, childNamespaceModifiedConfiguration);
  }

  /**
   * 返回灰度分支版本中，与主分支版本不同的配置项
   * @param masterReleaseConfigs    主分支版本配置项
   * @param branchReleaseConfigs    灰度分支版本的配置项
   * @param branchReleaseKeys       分支发布的时候修改的key。这个值仅在灰度发布的时候才会存储在ReleaseHistory中
   * @return
   */
  private Map<String, String> calculateBranchModifiedItemsAccordingToRelease(
      Map<String, String> masterReleaseConfigs, Map<String, String> branchReleaseConfigs,
      Collection<String> branchReleaseKeys) {

    Map<String, String> modifiedConfigs = new LinkedHashMap<>();

    if (CollectionUtils.isEmpty(branchReleaseConfigs)) {
      return modifiedConfigs;
    }

    // 新的逻辑。新逻辑中，通过传入的branchReleaseKeys我们已经知道了灰度发布版本涉及到的key
    // 因此只需要遍历这些key，从灰度分支版本的配置项中获取到这些key即可
    // new logic, retrieve modified configurations based on branch release keys
    if (branchReleaseKeys != null) {
      for (String branchReleaseKey : branchReleaseKeys) {
        if (branchReleaseConfigs.containsKey(branchReleaseKey)) {
          modifiedConfigs.put(branchReleaseKey, branchReleaseConfigs.get(branchReleaseKey));
        }
      }

      return modifiedConfigs;
    }

    // 这里是兼容老的逻辑，老的逻辑中没有传入branchReleaseKeys
    // 这种情况下，就需要遍历主版本和灰度版本的配置项，找到灰度版本和主版本不同的配置项
    // 组装成结果返回
    // old logic, retrieve modified configurations by comparing branchReleaseConfigs with masterReleaseConfigs
    if (CollectionUtils.isEmpty(masterReleaseConfigs)) {
      return branchReleaseConfigs;
    }

    for (Map.Entry<String, String> entry : branchReleaseConfigs.entrySet()) {

      if (!Objects.equals(entry.getValue(), masterReleaseConfigs.get(entry.getKey()))) {
        modifiedConfigs.put(entry.getKey(), entry.getValue());
      }
    }

    return modifiedConfigs;

  }

  @Transactional
  public int batchDelete(String appId, String clusterName, String namespaceName, String operator) {
    return releaseRepository.batchDelete(appId, clusterName, namespaceName, operator);
  }

}
