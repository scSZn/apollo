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
package com.ctrip.framework.apollo.portal.environment;

import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.NetUtil;
import com.ctrip.framework.apollo.portal.component.config.PortalConfig;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * <p>
 *     只用在Portal服务，用于提供一个可用的MetaServer的URL地址，如果给定环境中没有可用的MetaServer的URL地址，则默认会使用http://apollo.meta
 * </p>
 *
 * Only use in apollo-portal
 * Provider an available meta server url.
 * If there is no available meta server url for the given environment,
 * the default meta server url will be used(http://apollo.meta).
 * @see com.ctrip.framework.apollo.core.MetaDomainConsts
 * @author wxq
 */
@Service
public class PortalMetaDomainService {

    private static final Logger logger = LoggerFactory.getLogger(PortalMetaDomainService.class);

    /**
     * {@link PortalMetaDomainService#selectedMetaServerAddressCache} 的刷新时间，默认是60s一刷新
     */
    private static final long REFRESH_INTERVAL_IN_SECOND = 60;// 1 min

    /**
     * 默认的MetaServer地址
     */
    static final String DEFAULT_META_URL = "http://apollo.meta";

    /**
     * MetaServer的缓存<br>
     * key为环境<br/>
     * value为该环境下的MetaServer地址，如  http://1.1.1.1:8080,http://2.2.2.2:8080
     */
    private final Map<Env, String> metaServerAddressCache = Maps.newConcurrentMap();

    /**
     * <p>
     *     MetaServer的提供者，在 {@link PortalMetaDomainService#PortalMetaDomainService(com.ctrip.framework.apollo.portal.component.config.PortalConfig)} 中进行初始化
     * </p>
     *
     * initialize meta server provider without cache.
     * Multiple {@link PortalMetaServerProvider}
     */
    private final List<PortalMetaServerProvider> portalMetaServerProviders = new ArrayList<>();

    /**
     * key为多个MetaServer地址组成的逗号分隔的字符串<br/>
     * value为从key表示的多个MetaServer地址中选择的可用的MetaServer<br/>
     * 这个map会自动更新，更新时间为 {@link PortalMetaDomainService#REFRESH_INTERVAL_IN_SECOND} 的时间
     */
    // env -> meta server address cache
    // comma separated meta server address -> selected single meta server address cache
    private final Map<String, String> selectedMetaServerAddressCache = Maps.newConcurrentMap();

    /**
     * 标记定时刷新 {@link PortalMetaDomainService#selectedMetaServerAddressCache} 的定时任务是否启动，如果没有则需要刷新
     */
    private final AtomicBoolean periodicRefreshStarted = new AtomicBoolean(false);

    PortalMetaDomainService(final PortalConfig portalConfig) {
        // high priority with data in database
        portalMetaServerProviders.add(new DatabasePortalMetaServerProvider(portalConfig));

        // System properties, OS environment, configuration file
        portalMetaServerProviders.add(new DefaultPortalMetaServerProvider());
    }

    /**
     * 根据环境选择出一个MetaServer地址来，如果配置了多个MetaServer地址，则会挑选出一个<br>
     *
     * Return one meta server address. If multiple meta server addresses are configured, will select one.
     */
    public String getDomain(Env env) {
        String metaServerAddress = getMetaServerAddress(env);
        // if there is more than one address, need to select one
        if (metaServerAddress.contains(",")) {
            // 当配置了多个MetaServer地址时，只返回一个
            return selectMetaServerAddress(metaServerAddress);
        }
        return metaServerAddress;
    }

    /**
     * <p>
     *     返回目标环境的MetaServer地址，它返回的是配置的地址，即不是单个，不能直接使用，需要通过 {@link PortalMetaDomainService#updateMetaServerAddresses(String)} 进行一次选择后能进行使用
     * </p>
     * Return meta server address. If multiple meta server addresses are configured, will return the comma separated string.
     */
    public String getMetaServerAddress(Env env) {
        // in cache?
        if (!metaServerAddressCache.containsKey(env)) {
            // put it to cache
            metaServerAddressCache
                .put(env, getMetaServerAddressCacheValue(portalMetaServerProviders, env)
            );
        }

        // get from cache
        return metaServerAddressCache.get(env);
    }

    /**
     * <p>
     *     从配置的{@link PortalMetaDomainService#portalMetaServerProviders}中获取到指定env环境的MetaServer地址。
     *     如果没有找到，则会返回兜底的 {@link PortalMetaDomainService#DEFAULT_META_URL}值。
     *     运力就是遍历{@link PortalMetaDomainService#portalMetaServerProviders}，调用其中的 {@link PortalMetaServerProvider#getMetaServerAddress(Env)}方法，
     *     返回第一个非空的Provider的返回值
     * </p>
     *
     * Get the meta server from provider by given environment.
     * If there is no available meta server url for the given environment,
     * the default meta server url will be used(http://apollo.meta).
     * @param providers provide environment's meta server address
     * @param env environment
     * @return  meta server address
     */
    private String getMetaServerAddressCacheValue(
        Collection<PortalMetaServerProvider> providers, Env env) {

        // null value
        String metaAddress = null;

        for(PortalMetaServerProvider portalMetaServerProvider : providers) {
            if(portalMetaServerProvider.exists(env)) {
                metaAddress = portalMetaServerProvider.getMetaServerAddress(env);
                logger.info("Located meta server address [{}] for env [{}]", metaAddress, env);
                break;
            }
        }

        // check find it or not
        if (Strings.isNullOrEmpty(metaAddress)) {
            // Fallback to default meta address
            metaAddress = DEFAULT_META_URL;
            logger.warn(
                    "Meta server address fallback to [{}] for env [{}], because it is not available in MetaServerProvider",
                    metaAddress, env);
        }
        return metaAddress.trim();
    }

    /**
     * <p>
     *     调用每个MetaServerProvider的reload方法，刷新配置
     * </p>
     *
     * reload all {@link PortalMetaServerProvider}.
     * clear cache {@link PortalMetaDomainService#metaServerAddressCache}
     */
    public void reload() {
        for(PortalMetaServerProvider portalMetaServerProvider : portalMetaServerProviders) {
            portalMetaServerProvider.reload();
        }
        metaServerAddressCache.clear();
    }

    /**
     *
     *
     * Select one available meta server from the comma separated meta server addresses, e.g.
     * http://1.2.3.4:8080,http://2.3.4.5:8080
     *
     * <br />
     *
     * In production environment, we still suggest using one single domain like http://config.xxx.com(backed by software
     * load balancers like nginx) instead of multiple ip addresses
     *
     * <p>
     *     从输入的metaServerAddresses中挑选出一个MetaServer地址。
     *     在生成环境中推荐用域名来配置MetaServer，然后通过如nginx的方式进行负载均衡，而不是采用配置多个IP地址的方式
     * </p>
     * <p>
     *     这里的逻辑是，先从缓存 {@link PortalMetaDomainService#selectedMetaServerAddressCache} 查询结果，如果没查询到，则刷新缓存，同时如果没有启动定时更新的任务，
     *     还需要启动定时更新任务。然后从缓存中获取相应的值
     * </p>
     * <p>
     *
     * </p>
     *
     * @param metaServerAddresses 由多个MetaServer地址组成的，逗号分割的字符串，如   "http://1.1.1.1:8080,http://2.2.2.2:8080"
     */
    private String selectMetaServerAddress(String metaServerAddresses) {
        String metaAddressSelected = selectedMetaServerAddressCache.get(metaServerAddresses);
        if (metaAddressSelected == null) {
            // initialize
            if (periodicRefreshStarted.compareAndSet(false, true)) {
                schedulePeriodicRefresh();
            }
            updateMetaServerAddresses(metaServerAddresses);
            metaAddressSelected = selectedMetaServerAddressCache.get(metaServerAddresses);
        }

        return metaAddressSelected;
    }

    /**
     * 定期更新 {@link PortalMetaDomainService#selectedMetaServerAddressCache} 的值
     * <p>
     *     该函数作用就是，遍历输入的metaServerAddresses值，请求其中的 /services/config 接口，只要其中由一个成功返回了，则将其设置到{@link PortalMetaDomainService#selectedMetaServerAddressCache}中
     * </p>
     * @param metaServerAddresses 多个MetaServer地址组成的，由逗号分割的字符串
     */
    private void updateMetaServerAddresses(String metaServerAddresses) {
        logger.debug("Selecting meta server address for: {}", metaServerAddresses);

        Transaction transaction = Tracer.newTransaction("Apollo.MetaService", "refreshMetaServerAddress");
        transaction.addData("Url", metaServerAddresses);

        try {
            List<String> metaServers = Lists.newArrayList(metaServerAddresses.split(","));
            // random load balancing
            Collections.shuffle(metaServers);   // 打乱顺序已达到随机负载均衡的效果

            boolean serverAvailable = false;

            for (String address : metaServers) {
                address = address.trim();
                //check whether /services/config is accessible
                if (NetUtil.pingUrl(address + "/services/config")) {
                    // select the first available meta server
                    selectedMetaServerAddressCache.put(metaServerAddresses, address);
                    serverAvailable = true;
                    logger.debug("Selected meta server address {} for {}", address, metaServerAddresses);
                    break;
                }
            }

            // 这里是为了这种情况：如果metaServerAddresses中没有一个可用的服务，那么selectedMetaServerAddressCache
            // 就不会有有效的值，如果频繁请求，就会导致一直刷新缓存。可以参考缓存穿透的原理，放一个默认值进去，通过定时刷新达到最终一致性
            // we need to make sure the map is not empty, e.g. the first update might be failed
            if (!selectedMetaServerAddressCache.containsKey(metaServerAddresses)) {
                selectedMetaServerAddressCache.put(metaServerAddresses, metaServers.get(0).trim());
            }

            if (!serverAvailable) {
                logger.warn("Could not find available meta server for configured meta server addresses: {}, fallback to: {}",
                        metaServerAddresses, selectedMetaServerAddressCache.get(metaServerAddresses));
            }

            transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
            transaction.setStatus(ex);
            throw ex;
        } finally {
            transaction.complete();
        }
    }

    /**
     * 启动定时任务，定时刷新 {@link PortalMetaDomainService#selectedMetaServerAddressCache}
     */
    private void schedulePeriodicRefresh() {
        ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(1, ApolloThreadFactory.create("MetaServiceLocator", true));

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                for (String metaServerAddresses : selectedMetaServerAddressCache.keySet()) {
                    updateMetaServerAddresses(metaServerAddresses);
                }
            } catch (Throwable ex) {
                logger.warn(String.format("Refreshing meta server address failed, will retry in %d seconds",
                        REFRESH_INTERVAL_IN_SECOND), ex);
            }
        }, REFRESH_INTERVAL_IN_SECOND, REFRESH_INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

}
