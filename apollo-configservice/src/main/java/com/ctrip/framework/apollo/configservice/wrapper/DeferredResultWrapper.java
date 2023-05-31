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
package com.ctrip.framework.apollo.configservice.wrapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.List;
import java.util.Map;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DeferredResultWrapper implements Comparable<DeferredResultWrapper> {
  private static final ResponseEntity<List<ApolloConfigNotification>>
      NOT_MODIFIED_RESPONSE_LIST = new ResponseEntity<>(HttpStatus.NOT_MODIFIED);

  /**
   * key：一个Namespace的标准名称 <br>
   * value: Namespace的原来的名称 <br>
   * 这个map作用是，如果这个Namespace的名称变更了，客户端那边是不知晓的，仍然会传递旧的Namespace。
   * 而且客户端是根据 Namespace的名称来识别的，因此在返回的时候，需要用旧的Namespace返回给客户端
   */
  private Map<String, String> normalizedNamespaceNameToOriginalNamespaceName;
  private DeferredResult<ResponseEntity<List<ApolloConfigNotification>>> result;


  public DeferredResultWrapper(long timeoutInMilli) {
    result = new DeferredResult<>(timeoutInMilli, NOT_MODIFIED_RESPONSE_LIST);
  }

  public void recordNamespaceNameNormalizedResult(String originalNamespaceName, String normalizedNamespaceName) {
    if (normalizedNamespaceNameToOriginalNamespaceName == null) {
      normalizedNamespaceNameToOriginalNamespaceName = Maps.newHashMap();
    }
    normalizedNamespaceNameToOriginalNamespaceName.put(normalizedNamespaceName, originalNamespaceName);
  }


  public void onTimeout(Runnable timeoutCallback) {
    result.onTimeout(timeoutCallback);
  }

  public void onCompletion(Runnable completionCallback) {
    result.onCompletion(completionCallback);
  }


  public void setResult(ApolloConfigNotification notification) {
    setResult(Lists.newArrayList(notification));
  }

  /**
   * The namespace name is used as a key in client side, so we have to return the original one instead of the correct one
   */
  public void setResult(List<ApolloConfigNotification> notifications) {
    if (normalizedNamespaceNameToOriginalNamespaceName != null) {
      notifications.stream().filter(notification -> normalizedNamespaceNameToOriginalNamespaceName.containsKey
          (notification.getNamespaceName())).forEach(notification -> notification.setNamespaceName(
              normalizedNamespaceNameToOriginalNamespaceName.get(notification.getNamespaceName())));
    }

    result.setResult(new ResponseEntity<>(notifications, HttpStatus.OK));
  }

  public DeferredResult<ResponseEntity<List<ApolloConfigNotification>>> getResult() {
    return result;
  }

  @Override
  public int compareTo(@NonNull DeferredResultWrapper deferredResultWrapper) {
    return Integer.compare(this.hashCode(), deferredResultWrapper.hashCode());
  }
}
