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


import com.ctrip.framework.apollo.common.dto.AppDTO;
import com.ctrip.framework.apollo.common.entity.App;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.http.MultiResponseEntity;
import com.ctrip.framework.apollo.common.http.RichResponseEntity;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.portal.component.PortalSettings;
import com.ctrip.framework.apollo.portal.enricher.adapter.AppDtoUserInfoEnrichedAdapter;
import com.ctrip.framework.apollo.portal.entity.model.AppModel;
import com.ctrip.framework.apollo.portal.entity.po.Role;
import com.ctrip.framework.apollo.portal.entity.vo.EnvClusterInfo;
import com.ctrip.framework.apollo.portal.environment.Env;
import com.ctrip.framework.apollo.portal.listener.AppCreationEvent;
import com.ctrip.framework.apollo.portal.listener.AppDeletionEvent;
import com.ctrip.framework.apollo.portal.listener.AppInfoChangedEvent;
import com.ctrip.framework.apollo.portal.service.AdditionalUserInfoEnrichService;
import com.ctrip.framework.apollo.portal.service.AppService;
import com.ctrip.framework.apollo.portal.service.RoleInitializationService;
import com.ctrip.framework.apollo.portal.service.RolePermissionService;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import com.ctrip.framework.apollo.portal.util.RoleUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;

import javax.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;


@RestController
@RequestMapping("/apps")
public class AppController {

  private final UserInfoHolder userInfoHolder;
  private final AppService appService;
  private final PortalSettings portalSettings;
  private final ApplicationEventPublisher publisher;
  private final RolePermissionService rolePermissionService;
  private final RoleInitializationService roleInitializationService;
  private final AdditionalUserInfoEnrichService additionalUserInfoEnrichService;

  public AppController(
      final UserInfoHolder userInfoHolder,
      final AppService appService,
      final PortalSettings portalSettings,
      final ApplicationEventPublisher publisher,
      final RolePermissionService rolePermissionService,
      final RoleInitializationService roleInitializationService,
      final AdditionalUserInfoEnrichService additionalUserInfoEnrichService) {
    this.userInfoHolder = userInfoHolder;
    this.appService = appService;
    this.portalSettings = portalSettings;
    this.publisher = publisher;
    this.rolePermissionService = rolePermissionService;
    this.roleInitializationService = roleInitializationService;
    this.additionalUserInfoEnrichService = additionalUserInfoEnrichService;
  }

  /**
   * 查找APP，如果没有输入任何参数，则代表查找所有的APP
   * <p>
   *     实现方式：实现上没有什么特殊的地方，就是直接查询数据库获取相应的APP信息
   * </p>
   * @param appIds  查询指定的appId，多个appId用逗号连接起来
   * @return
   */
  @GetMapping
  public List<App> findApps(@RequestParam(value = "appIds", required = false) String appIds) {
    if (Strings.isNullOrEmpty(appIds)) {
      return appService.findAll();
    }
    return appService.findByAppIds(Sets.newHashSet(appIds.split(",")));
  }

  /**
   * 查询APP信息
   * <p>
   *     实现方式：
   *     <ol>
   *         <li>
   *             通过 {@link com.ctrip.framework.apollo.portal.service.RolePermissionService#findUserRoles(String)} 查询该人员拥有的角色
   *         </li>
   *         <li>
   *             从角色中提取出APPID信息，然后查询这些APP
   *         </li>
   *     </ol>
   * </p>
   * @param owner 用户ID（即登录名）
   * @param page  分页相关参数
   * @return
   */
  @GetMapping("/by-owner")
  public List<App> findAppsByOwner(@RequestParam("owner") String owner, Pageable page) {
    Set<String> appIds = Sets.newHashSet();

    List<Role> userRoles = rolePermissionService.findUserRoles(owner);

    for (Role role : userRoles) {
      String appId = RoleUtils.extractAppIdFromRoleName(role.getRoleName());

      if (appId != null) {
        appIds.add(appId);
      }
    }

    return appService.findByAppIds(appIds, page);
  }

  /**
   * 创建APP，分为两步骤<br/>
   * <ol>
   *     <li>在本地数据库保存APP信息</li>
   *     <li>发送消息，通过消息异步的将App信息发送给AdminService，让它保存APP，见{@link com.ctrip.framework.apollo.portal.listener.CreationListener}和{@link com.ctrip.framework.apollo.portal.api.AdminServiceAPI.AppAPI}</li>
   *     <li>给所选的APP管理员授权</li>
   * </ol>
   *
   * 对于可能存在的AdminService没有同步创建APP的问题，会有补偿机制，在APP的管理界面上有一个补偿环境的按钮，通过它来补偿
   *
   * @param appModel  创建APP所需的参数信息，包括所属部门，APPID，名称，还有管理员等信息
   * @return  创建成功后的APP信息
   */
  @PreAuthorize(value = "@permissionValidator.hasCreateApplicationPermission()")
  @PostMapping
  public App create(@Valid @RequestBody AppModel appModel) {

    App app = transformToApp(appModel);

    App createdApp = appService.createAppInLocal(app);

    publisher.publishEvent(new AppCreationEvent(createdApp));

    Set<String> admins = appModel.getAdmins();
    if (!CollectionUtils.isEmpty(admins)) {
      rolePermissionService
          .assignRoleToUsers(RoleUtils.buildAppMasterRoleName(createdApp.getAppId()),
              admins, userInfoHolder.getUser().getUserId());
    }

    return createdApp;
  }

  /**
   * 更新APP的信息，同创建APP步骤差不多
   * @param appId
   * @param appModel
   */
  @PreAuthorize(value = "@permissionValidator.isAppAdmin(#appId)")
  @PutMapping("/{appId:.+}")
  public void update(@PathVariable String appId, @Valid @RequestBody AppModel appModel) {
    if (!Objects.equals(appId, appModel.getAppId())) {
      throw new BadRequestException("The App Id of path variable and request body is different");
    }

    App app = transformToApp(appModel);

    App updatedApp = appService.updateAppInLocal(app);

    publisher.publishEvent(new AppInfoChangedEvent(updatedApp));
  }

  /**
   * 获取某个APP左侧环境列表的导航菜单
   *
   * @param appId    APP的ID
   * @return  环境及其对应的集群<br/>
   * 一般来讲，创建APP的时候，会在每个环境下创建一个默认的集群<br/>
   * 如果某个环境没有任何集群（不正常情况），则不会展示该集群，且会报错400<br/>
   * 如果某个环境只有一个default集群，则不会将该环境展开<br/>
   * 如果某个环境超过两个集群，才会将该环境展开，显示其所有的集群
   *
   */
  @GetMapping("/{appId}/navtree")
  public MultiResponseEntity<EnvClusterInfo> nav(@PathVariable String appId) {

    MultiResponseEntity<EnvClusterInfo> response = MultiResponseEntity.ok();
    List<Env> envs = portalSettings.getActiveEnvs();
    for (Env env : envs) {
      try {
        response.addResponseEntity(RichResponseEntity.ok(appService.createEnvNavNode(env, appId)));
      } catch (Exception e) {
        response.addResponseEntity(RichResponseEntity.error(HttpStatus.INTERNAL_SERVER_ERROR,
            "load env:" + env.getName() + " cluster error." + e
                .getMessage()));
      }
    }
    return response;
  }

  /**
   * 补偿机制，主要用于在创建APP的时候，可能某个环境创建失败的情况<br/>
   * 只在远端AdminService创建
   *
   * @param env 补偿环境
   * @param app APP信息
   * @return
   */
  @PostMapping(value = "/envs/{env}", consumes = {"application/json"})
  public ResponseEntity<Void> create(@PathVariable String env, @Valid @RequestBody App app) {
    appService.createAppInRemote(Env.valueOf(env), app);

    roleInitializationService.initNamespaceSpecificEnvRoles(app.getAppId(), ConfigConsts.NAMESPACE_APPLICATION,
            env, userInfoHolder.getUser().getUserId());

    return ResponseEntity.ok().build();
  }

  /**
   * 加载指定APP的详细信息
   *
   * @param appId  APPID
   * @return
   */
  @GetMapping("/{appId:.+}")
  public AppDTO load(@PathVariable String appId) {
    App app = appService.load(appId);
    AppDTO appDto = BeanUtils.transform(AppDTO.class, app);
    additionalUserInfoEnrichService.enrichAdditionalUserInfo(Collections.singletonList(appDto),
        AppDtoUserInfoEnrichedAdapter::new);
    return appDto;
  }

  /**
   * 删除指定APP。同时需要将信息同步到AdminService，通过事件机制<br/>
   * 见 {@link com.ctrip.framework.apollo.portal.listener.DeletionListener#onAppDeletionEvent(com.ctrip.framework.apollo.portal.listener.AppDeletionEvent)}
   * @param appId
   */
  @PreAuthorize(value = "@permissionValidator.isSuperAdmin()")
  @DeleteMapping("/{appId:.+}")
  public void deleteApp(@PathVariable String appId) {
    App app = appService.deleteAppInLocal(appId);

    publisher.publishEvent(new AppDeletionEvent(app));
  }

  /**
   * 查询该APP在哪个环境中不存在，可能是创建失败，或者是后来接入的环境<br/>
   * 该API的实现是通过遍历所有的env，尝试去对应的AdminService获取APP信息，如果没有获取到（404），则说明在该环境中没有创建成功
   *
   * @param appId 指定APP的ID
   * @return
   */
  @GetMapping("/{appId}/miss_envs")
  public MultiResponseEntity<String> findMissEnvs(@PathVariable String appId) {

    MultiResponseEntity<String> response = MultiResponseEntity.ok();
    for (Env env : portalSettings.getActiveEnvs()) {
      try {
        appService.load(env, appId);
      } catch (Exception e) {
        if (e instanceof HttpClientErrorException &&
            ((HttpClientErrorException) e).getStatusCode() == HttpStatus.NOT_FOUND) {
          response.addResponseEntity(RichResponseEntity.ok(env.toString()));
        } else {
          response.addResponseEntity(RichResponseEntity.error(HttpStatus.INTERNAL_SERVER_ERROR,
              String.format("load appId:%s from env %s error.", appId,
                  env)
                  + e.getMessage()));
        }
      }
    }

    return response;
  }

  private App transformToApp(AppModel appModel) {
    String appId = appModel.getAppId();
    String appName = appModel.getName();
    String ownerName = appModel.getOwnerName();
    String orgId = appModel.getOrgId();
    String orgName = appModel.getOrgName();

    return App.builder()
        .appId(appId)
        .name(appName)
        .ownerName(ownerName)
        .orgId(orgId)
        .orgName(orgName)
        .build();

  }
}
