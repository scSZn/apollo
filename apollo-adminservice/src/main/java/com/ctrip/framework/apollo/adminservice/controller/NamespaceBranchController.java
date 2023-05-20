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
package com.ctrip.framework.apollo.adminservice.controller;

import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.message.MessageSender;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.NamespaceBranchService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NamespaceBranchController {

  private final MessageSender messageSender;
  private final NamespaceBranchService namespaceBranchService;
  private final NamespaceService namespaceService;

  public NamespaceBranchController(
      final MessageSender messageSender,
      final NamespaceBranchService namespaceBranchService,
      final NamespaceService namespaceService) {
    this.messageSender = messageSender;
    this.namespaceBranchService = namespaceBranchService;
    this.namespaceService = namespaceService;
  }

  /**
   * 创建灰度分支
   * <p>
   *     关于Namespace灰度版本的数据结构，其实是通过Cluster来标识的。
   *     当创建灰度Namespace的时候，实际上是创建了一个新的Cluster，这个Cluster的parentClusterId就是原Cluster的ID，
   *     先称之为子Cluster，然后在这个子Cluster下创建一个新的Namespace。也就是说，这个灰度版本的Namespace其实是不知道自己是灰度分支
   * </p>
   * @param appId           应用ID
   * @param clusterName     集群名称
   * @param namespaceName   命名空间名称
   * @param operator        操作人名称
   * @return
   */
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches")
  public NamespaceDTO createBranch(@PathVariable String appId,
                                   @PathVariable String clusterName,
                                   @PathVariable String namespaceName,
                                   @RequestParam("operator") String operator) {
    // 1. 判断命名空间是否存在
    checkNamespace(appId, clusterName, namespaceName);
    // 2. 创建灰度分支
    Namespace createdBranch = namespaceBranchService.createBranch(appId, clusterName, namespaceName, operator);

    return BeanUtils.transform(NamespaceDTO.class, createdBranch);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}/rules")
  public GrayReleaseRuleDTO findBranchGrayRules(@PathVariable String appId,
                                                @PathVariable String clusterName,
                                                @PathVariable String namespaceName,
                                                @PathVariable String branchName) {

    checkBranch(appId, clusterName, namespaceName, branchName);

    GrayReleaseRule rules = namespaceBranchService.findBranchGrayRules(appId, clusterName, namespaceName, branchName);
    if (rules == null) {
      return null;
    }
    GrayReleaseRuleDTO ruleDTO =
        new GrayReleaseRuleDTO(rules.getAppId(), rules.getClusterName(), rules.getNamespaceName(),
                               rules.getBranchName());

    ruleDTO.setReleaseId(rules.getReleaseId());

    ruleDTO.setRuleItems(GrayReleaseRuleItemTransformer.batchTransformFromJSON(rules.getRules()));

    return ruleDTO;
  }

  @Transactional
  @PutMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}/rules")
  public void updateBranchGrayRules(@PathVariable String appId, @PathVariable String clusterName,
                                    @PathVariable String namespaceName, @PathVariable String branchName,
                                    @RequestBody GrayReleaseRuleDTO newRuleDto) {

    checkBranch(appId, clusterName, namespaceName, branchName);

    GrayReleaseRule newRules = BeanUtils.transform(GrayReleaseRule.class, newRuleDto);
    newRules.setRules(GrayReleaseRuleItemTransformer.batchTransformToJSON(newRuleDto.getRuleItems()));
    newRules.setBranchStatus(NamespaceBranchStatus.ACTIVE);

    namespaceBranchService.updateBranchGrayRules(appId, clusterName, namespaceName, branchName, newRules);

    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
  }

  @Transactional
  @DeleteMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches/{branchName}")
  public void deleteBranch(@PathVariable String appId, @PathVariable String clusterName,
                           @PathVariable String namespaceName, @PathVariable String branchName,
                           @RequestParam("operator") String operator) {

    checkBranch(appId, clusterName, namespaceName, branchName);

    namespaceBranchService
        .deleteBranch(appId, clusterName, namespaceName, branchName, NamespaceBranchStatus.DELETED, operator);

    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);

  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/branches")
  public NamespaceDTO loadNamespaceBranch(@PathVariable String appId, @PathVariable String clusterName,
                                          @PathVariable String namespaceName) {

    checkNamespace(appId, clusterName, namespaceName);

    Namespace childNamespace = namespaceBranchService.findBranch(appId, clusterName, namespaceName);
    if (childNamespace == null) {
      return null;
    }

    return BeanUtils.transform(NamespaceDTO.class, childNamespace);
  }

  private void checkBranch(String appId, String clusterName, String namespaceName, String branchName) {
    //1. check parent namespace
    checkNamespace(appId, clusterName, namespaceName);

    //2. check child namespace
    Namespace childNamespace = namespaceService.findOne(appId, branchName, namespaceName);
    if (childNamespace == null) {
      throw new BadRequestException(
          "Namespace's branch not exist. AppId = %s, ClusterName = %s, NamespaceName = %s, BranchName = %s",
          appId, clusterName, namespaceName, branchName);
    }

  }

  private void checkNamespace(String appId, String clusterName, String namespaceName) {
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (parentNamespace == null) {
      throw BadRequestException.namespaceNotExists(appId, clusterName, namespaceName);
    }
  }


}
