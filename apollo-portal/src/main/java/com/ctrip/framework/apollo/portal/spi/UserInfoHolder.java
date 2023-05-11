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

import com.ctrip.framework.apollo.portal.entity.bo.UserInfo;

/**
 * <p>
 *     定义了获取用户信息的接口，不同的公司可以有不同的实现<br>
 *     Apollo默认提供了三种实现
 *     <ul>
 *         <li>
 *             {@link com.ctrip.framework.apollo.portal.spi.defaultimpl.DefaultUserInfoHolder} 默认的获取用户信息的类，
 *             它提供了一个Apollo用户，每次都只会返回这个类型。其如何生效可见{@link com.ctrip.framework.apollo.portal.spi.configuration.AuthConfiguration.DefaultAuthAutoConfiguration#defaultUserInfoHolder()}
 *         </li>
 *         <li>
 *              {@link com.ctrip.framework.apollo.portal.spi.oidc.OidcUserInfoHolder} OIDC协议实现的UserInfoHolder
 *         </li>
 *         <li>
 *             {@link com.ctrip.framework.apollo.portal.spi.springsecurity.SpringSecurityUserInfoHolder} 集成了SpringSecurity的UserInfoHolder
 *         </li>
 *     </ul>
 * </p>
 *
 * Get access to the user's information,
 * different companies should have a different implementation
 */
public interface UserInfoHolder {

  UserInfo getUser();

}
