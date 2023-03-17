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
package com.ctrip.framework.apollo.portal.controller;

import com.ctrip.framework.apollo.common.dto.ClusterDTO;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.portal.service.ClusterService;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
public class ClusterController {

  private final ClusterService clusterService;
  private final UserInfoHolder userInfoHolder;

  public ClusterController(final ClusterService clusterService, final UserInfoHolder userInfoHolder) {
    this.clusterService = clusterService;
    this.userInfoHolder = userInfoHolder;
  }

  /**
   * 创建集群
   * <ul>
   *     <li>判断集群名称在指定的env中是否重复，如果重复，抛出异常</li>
   *     <li>同步调用对应env中的AdminService服务进行创建集群</li>
   * </ul>
   *
   * 可以看到，创建集群和创建APP是不一样的，创建APP是异步的方式，先在本地创建，然后通过消息的方式让每个env创建相应的APP <br/>
   * 创建集群则是直接同步调用相应env中的AdminService服务进行创建，且Portal服务不会存储任何集群信息
   * @param appId     在哪个APP中进行创建
   * @param env       在哪个环境下进行创建
   * @param cluster   集群信息
   * @return
   */
  @PreAuthorize(value = "@permissionValidator.hasCreateClusterPermission(#appId)")
  @PostMapping(value = "apps/{appId}/envs/{env}/clusters")
  public ClusterDTO createCluster(@PathVariable String appId, @PathVariable String env,
                                  @Valid @RequestBody ClusterDTO cluster) {
    String operator = userInfoHolder.getUser().getUserId();
    cluster.setDataChangeLastModifiedBy(operator);
    cluster.setDataChangeCreatedBy(operator);

    return clusterService.createCluster(Env.valueOf(env), cluster);
  }

  /**
   * 删除集群
   * @param appId
   * @param env
   * @param clusterName
   * @return
   */
  @PreAuthorize(value = "@permissionValidator.isSuperAdmin()")
  @DeleteMapping(value = "apps/{appId}/envs/{env}/clusters/{clusterName:.+}")
  public ResponseEntity<Void> deleteCluster(@PathVariable String appId, @PathVariable String env,
                                            @PathVariable String clusterName){
    clusterService.deleteCluster(Env.valueOf(env), appId, clusterName);
    return ResponseEntity.ok().build();
  }

  /**
   * 获取指定APP下指定env的指定集群名称详细信息，这里也是同步调用的方式，因为portal中没有存储任何集群信息
   *
   * @param appId         指定APPID
   * @param env           环境
   * @param clusterName   集群名称
   * @return
   */
  @GetMapping(value = "apps/{appId}/envs/{env}/clusters/{clusterName:.+}")
  public ClusterDTO loadCluster(@PathVariable("appId") String appId, @PathVariable String env, @PathVariable("clusterName") String clusterName) {

    return clusterService.loadCluster(appId, Env.valueOf(env), clusterName);
  }

}
