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

import com.ctrip.framework.apollo.biz.entity.Cluster;
import com.ctrip.framework.apollo.biz.service.ClusterService;
import com.ctrip.framework.apollo.common.dto.ClusterDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.ConfigConsts;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

@RestController
public class ClusterController {

  private final ClusterService clusterService;

  public ClusterController(final ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  /**
   * 创建集群
   * @param appId                         对哪个APP进行创建集群
   * @param autoCreatePrivateNamespace    是否根据AppNamespace自动创建私有Namespace
   * @param dto                           集群相关信息
   * @return
   */
  @PostMapping("/apps/{appId}/clusters")
  public ClusterDTO create(@PathVariable("appId") String appId,
                           @RequestParam(value = "autoCreatePrivateNamespace", defaultValue = "true") boolean autoCreatePrivateNamespace,
                           @Valid @RequestBody ClusterDTO dto) {
    Cluster entity = BeanUtils.transform(Cluster.class, dto);
    // 1. 判断集群是否重复了。每个App下的集群名称必须唯一
    Cluster managedEntity = clusterService.findOne(appId, entity.getName());
    if (managedEntity != null) {
      throw BadRequestException.clusterAlreadyExists(entity.getName());
    }

    // 2. 根据是否自动创建相应的Namespace调用不同的方法
    if (autoCreatePrivateNamespace) {
      entity = clusterService.saveWithInstanceOfAppNamespaces(entity);
    } else {
      entity = clusterService.saveWithoutInstanceOfAppNamespaces(entity);
    }

    return BeanUtils.transform(ClusterDTO.class, entity);
  }

  /**
   * 删除集群。不允许删除默认集群，默认集群只有在删除APP的时候一起删除
   * @param appId
   * @param clusterName
   * @param operator
   */
  @DeleteMapping("/apps/{appId}/clusters/{clusterName:.+}")
  public void delete(@PathVariable("appId") String appId,
                     @PathVariable("clusterName") String clusterName, @RequestParam String operator) {

    Cluster entity = clusterService.findOne(appId, clusterName);

    if (entity == null) {
      throw NotFoundException.clusterNotFound(appId, clusterName);
    }

    if(ConfigConsts.CLUSTER_NAME_DEFAULT.equals(entity.getName())){
      throw new BadRequestException("can not delete default cluster!");
    }

    clusterService.delete(entity.getId(), operator);
  }

  /**
   * 查询指定APP下的所有集群。注意是非灰度版本的集群
   * @param appId   APPID
   * @return
   */
  @GetMapping("/apps/{appId}/clusters")
  public List<ClusterDTO> find(@PathVariable("appId") String appId) {
    List<Cluster> clusters = clusterService.findParentClusters(appId);
    return BeanUtils.batchTransform(ClusterDTO.class, clusters);
  }

  /**
   * 查询指定APP的指定集群名称的集群信息。这里的集群名称是精确匹配
   * @param appId
   * @param clusterName
   * @return
   */
  @GetMapping("/apps/{appId}/clusters/{clusterName:.+}")
  public ClusterDTO get(@PathVariable("appId") String appId,
                        @PathVariable("clusterName") String clusterName) {
    Cluster cluster = clusterService.findOne(appId, clusterName);
    if (cluster == null) {
      throw NotFoundException.clusterNotFound(appId, clusterName);
    }
    return BeanUtils.transform(ClusterDTO.class, cluster);
  }

  /**
   * 判断该集群名称在其应用中是否唯一
   * @param appId
   * @param clusterName
   * @return
   */
  @GetMapping("/apps/{appId}/cluster/{clusterName}/unique")
  public boolean isAppIdUnique(@PathVariable("appId") String appId,
                               @PathVariable("clusterName") String clusterName) {
    return clusterService.isClusterNameUnique(appId, clusterName);
  }
}
