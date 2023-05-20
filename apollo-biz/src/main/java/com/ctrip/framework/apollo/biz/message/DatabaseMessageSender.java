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

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *     依靠数据库实现的消息发布者实现类。通过往数据库中插入记录，然后定时扫描这些记录，将对应的发布消息同步给相应的服务实例。扫描通知的功能由
 *     {@link ReleaseMessageListener} 实现
 * </p>
 * <p>
 *     发布消息的数据结构非常简单，仅有ID，message，数据最后修改时间三个。其中ID是自增ID，message在发布配置的场景下为 appID,clusterName,namespaceName 组合而成。也就是说，如果同一个Namespace的不同发布消息，它们的message是相同的
 * </p>
 * <p>
 *     理论上来讲，每一次发布，都会增加一条发布消息，然后通知相应的服务实例拉取最新的配置。但是如果用户在极短的时间内，对某个Namespace进行多次发布，就会触发多次更新，但实际每次拉取到的都是一样配置。
 *     因此为了降低服务实例拉取最新配置实例，DatabaseMessageSender中有一个定时任务，定时删除掉相同Namespace的重复的发布记录，只保留最新的一条
 * </p>
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DatabaseMessageSender implements MessageSender {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
  private static final int CLEAN_QUEUE_MAX_SIZE = 100;
  /**
   * 这里保留了发布记录ID，cleanExecutorService会从中获取到发布记录ID，然后删除掉与该记录ID相同的发布记录的其他老旧发布记录
   */
  private final BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
  private final ExecutorService cleanExecutorService;
  private final AtomicBoolean cleanStopped;

  private final ReleaseMessageRepository releaseMessageRepository;

  public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
    cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
    cleanStopped = new AtomicBoolean(false);
    this.releaseMessageRepository = releaseMessageRepository;
  }

  /**
   * 发送发布消息。这里是往数据库中添加一条发布消息的记录
   * @param message   消息内容，一般是  appid,clusterName,namespaceName
   * @param channel   发送给哪个渠道，类似于kafka的Topic，目前仅有{@link Topics#APOLLO_RELEASE_TOPIC}
   */
  @Override
  @Transactional
  public void sendMessage(String message, String channel) {
    logger.info("Sending message {} to channel {}", message, channel);
    if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
      logger.warn("Channel {} not supported by DatabaseMessageSender!", channel);
      return;
    }

    Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
    Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
    try {
      ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
      if(!toClean.offer(newMessage.getId())){
        logger.warn("Queue is full, Failed to add message {} to clean queue", newMessage.getId());
      }
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      logger.error("Sending message to database failed", ex);
      transaction.setStatus(ex);
      throw ex;
    } finally {
      transaction.complete();
    }
  }

  /**
   * 初始化方法，这里是循环遍历toClean队列，清理掉其中重复的发布记录
   */
  @PostConstruct
  private void initialize() {
    cleanExecutorService.submit(() -> {
      while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // 从队列中获取发布记录ID，设置1秒钟超时，如果没有获取到，则等待5秒钟
          Long rm = toClean.poll(1, TimeUnit.SECONDS);
          if (rm != null) {
            cleanMessage(rm);
          } else {
            TimeUnit.SECONDS.sleep(5);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  /**
   * 清理发布消息
   * <p>
   *     清理的规则为，查找到指定ID的发布消息。根据message查找到相同message的其他发布消息，保留最新的发布消息，删除所有旧的发布消息
   * </p>
   * @param id
   */
  private void cleanMessage(Long id) {
    //double check in case the release message is rolled back
    ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
    if (releaseMessage == null) {
      return;
    }
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
          releaseMessage.getMessage(), releaseMessage.getId());

      releaseMessageRepository.deleteAll(messages);
      hasMore = messages.size() == 100;

      messages.forEach(toRemove -> Tracer.logEvent(
          String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
    }
  }

  @PreDestroy
  void stopClean() {
    cleanStopped.set(true);
  }
}
