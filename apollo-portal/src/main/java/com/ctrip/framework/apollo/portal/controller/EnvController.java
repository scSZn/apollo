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

import com.ctrip.framework.apollo.portal.component.PortalSettings;
import com.ctrip.framework.apollo.portal.environment.Env;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/envs")
public class EnvController {

  private final PortalSettings portalSettings;

  public EnvController(final PortalSettings portalSettings) {
    this.portalSettings = portalSettings;
  }

  /**
   * <h2>获取可用的环境</h4>
   * <h3>原理</h5>
   * <p>
   *     通过配置apollo.portal.envs的值来设置支持的环境，每个环境按照逗号分隔起来，如配置了 dev,test 则有两个环境，dev环境和test环境。
   *     其中，apollo.portal.envs的读取涉及到以下类
   *     <ul>
   *         <li>{@link com.ctrip.framework.apollo.portal.component.PortalSettings} 顾名思义，Portal服务的设置类，实际功能比较少，主要是检测各个环境的admin服务是否可用，并负责剔除掉不可用的环境</li>
   *         <li>{@link com.ctrip.framework.apollo.portal.component.config.PortalConfig} 是Portal服务的配置类，从中可以读取Portal服务的所有配置</li>
   *         <li>{@link com.ctrip.framework.apollo.portal.service.PortalDBPropertySource} 数据库的配置数据源，从数据库中读取Portal服务的配置</li>
   *     </ul>
   * </p>
   * @return
   */
  @GetMapping
  public List<String> envs() {
    List<String> environments = new ArrayList<>();
    for(Env env : portalSettings.getActiveEnvs()) {
      environments.add(env.toString());
    }
    return environments;
  }

}
