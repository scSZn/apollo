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

import java.util.List;

/**
 * <p>
 *     用户相关查询接口定义。包含以下实现类
 *     <ul>
 *         <li>
 *             {@link com.ctrip.framework.apollo.portal.spi.defaultimpl.DefaultUserService} 默认的用户服务，只会返回默认用户apollo的信息
 *         </li>
 *         <li>
 *
 *         </li>
 *     </ul>
 * </p>
 * @author Jason Song(song_s@ctrip.com)
 */
public interface UserService {
  /**
   * 根据关键字查询用户
   * @param keyword   关键字
   * @param offset    偏移量
   * @param limit     查询多少用户
   * @param includeInactiveUsers  是否包含禁用用户
   * @return
   */
  List<UserInfo> searchUsers(String keyword, int offset, int limit, boolean includeInactiveUsers);

  /**
   * 根据用户ID（即登录名）查询用户
   * @param userId
   * @return
   */
  UserInfo findByUserId(String userId);

  /**
   * 批量查询用户
   * @param userIds
   * @return
   */
  List<UserInfo> findByUserIds(List<String> userIds);

}
