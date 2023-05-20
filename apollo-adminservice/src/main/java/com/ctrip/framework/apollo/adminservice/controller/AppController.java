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

import com.ctrip.framework.apollo.biz.service.AdminService;
import com.ctrip.framework.apollo.biz.service.AppService;
import com.ctrip.framework.apollo.common.dto.AppDTO;
import com.ctrip.framework.apollo.common.entity.App;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;
import java.util.Objects;

@RestController
public class AppController {

  private final AppService appService;
  private final AdminService adminService;

  public AppController(final AppService appService, final AdminService adminService) {
    this.appService = appService;
    this.adminService = adminService;
  }

  /**
   * 创建APP，核心功能都在 {@link com.ctrip.framework.apollo.biz.service.AdminService#createNewApp(com.ctrip.framework.apollo.common.entity.App)}
   * @param dto
   * @return
   */
  @PostMapping("/apps")
  public AppDTO create(@Valid @RequestBody AppDTO dto) {
    App entity = BeanUtils.transform(App.class, dto);
    App managedEntity = appService.findOne(entity.getAppId());
    if (managedEntity != null) {
      throw BadRequestException.appAlreadyExists(entity.getAppId());
    }

    entity = adminService.createNewApp(entity);

    return BeanUtils.transform(AppDTO.class, entity);
  }

  /**
   * 删除指定APP
   * @param appId
   * @param operator
   */
  @DeleteMapping("/apps/{appId:.+}")
  public void delete(@PathVariable("appId") String appId, @RequestParam String operator) {
    App entity = appService.findOne(appId);
    if (entity == null) {
      throw NotFoundException.appNotFound(appId);
    }
    adminService.deleteApp(entity, operator);
  }

  /**
   * 更新应用信息
   * @param appId
   * @param app
   */
  @PutMapping("/apps/{appId:.+}")
  public void update(@PathVariable String appId, @RequestBody App app) {
    if (!Objects.equals(appId, app.getAppId())) {
      throw new BadRequestException("The App Id of path variable and request body is different");
    }

    appService.update(app);
  }

  /**
   * 根据名称查询APP。模糊查询
   * @param name
   * @param pageable
   * @return
   */
  @GetMapping("/apps")
  public List<AppDTO> find(@RequestParam(value = "name", required = false) String name,
                           Pageable pageable) {
    List<App> app = null;
    if (StringUtils.isBlank(name)) {
      app = appService.findAll(pageable);
    } else {
      app = appService.findByName(name);
    }
    return BeanUtils.batchTransform(AppDTO.class, app);
  }

  /**
   * 根据应用ID查询APP。精确查询
   * @param appId
   * @return
   */
  @GetMapping("/apps/{appId:.+}")
  public AppDTO get(@PathVariable("appId") String appId) {
    App app = appService.findOne(appId);
    if (app == null) {
      throw NotFoundException.appNotFound(appId);
    }
    return BeanUtils.transform(AppDTO.class, app);
  }

  /**
   * 判断该APPID是否被使用
   * @param appId
   * @return
   */
  @GetMapping("/apps/{appId}/unique")
  public boolean isAppIdUnique(@PathVariable("appId") String appId) {
    return appService.isAppIdUnique(appId);
  }
}
