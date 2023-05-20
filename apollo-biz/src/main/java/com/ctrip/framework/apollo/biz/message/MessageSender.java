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
package com.ctrip.framework.apollo.biz.message;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface MessageSender {

  /**
   * 发送发布消息
   * @param message   消息内容，一般是  appid,clusterName,namespaceName
   * @param channel   发送给哪个渠道，类似于kafka的Topic，目前仅有{@link Topics#APOLLO_RELEASE_TOPIC}
   */
  void sendMessage(String message, String channel);
}
