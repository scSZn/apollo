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

import com.ctrip.framework.apollo.adminservice.aop.PreAcquireNamespaceLock;
import com.ctrip.framework.apollo.biz.service.ItemSetService;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ItemSetController {

  private final ItemSetService itemSetService;

  public ItemSetController(final ItemSetService itemSetService) {
    this.itemSetService = itemSetService;
  }

  /**
   * 通过文本编辑的方式修改配置项的时候，会调用到这个接口进行更新配置
   * @param appId             应用ID
   * @param clusterName       集群名称
   * @param namespaceName     命名空间
   * @param changeSet         配置项
   * @return
   */
  @PreAcquireNamespaceLock
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/itemset")
  public ResponseEntity<Void> create(@PathVariable String appId, @PathVariable String clusterName,
                                     @PathVariable String namespaceName, @RequestBody ItemChangeSets changeSet) {

    itemSetService.updateSet(appId, clusterName, namespaceName, changeSet);

    return ResponseEntity.status(HttpStatus.OK).build();
  }


}
