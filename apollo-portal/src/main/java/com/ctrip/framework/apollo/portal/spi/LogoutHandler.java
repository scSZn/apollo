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
package com.ctrip.framework.apollo.portal.spi;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * <p>
 *     登出的处理器，只有一个logout方法。目前只有两个实现类
 *     <ul>
 *         <li>
 *             {@link com.ctrip.framework.apollo.portal.spi.defaultimpl.DefaultLogoutHandler}默认的登出处理器，
 *         只有一个功能，重定向到首页，其他不做处理
 *         </li>
 *         <li>
 *            {@link com.ctrip.framework.apollo.portal.spi.oidc.OidcLogoutHandler}OIDC协议的登出处理器，目前看没有实现，是空方法 2023-05-09
 *         </li>
 *     </ul>
 * </p>
 */
public interface LogoutHandler {

  void logout(HttpServletRequest request, HttpServletResponse response);

}
