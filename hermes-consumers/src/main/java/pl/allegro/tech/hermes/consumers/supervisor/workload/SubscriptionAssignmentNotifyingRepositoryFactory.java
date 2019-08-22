package pl.allegro.tech.hermes.consumers.supervisor.workload;

import org.apache.curator.framework.CuratorFramework;
import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

import javax.inject.Inject;

public class SubscriptionAssignmentNotifyingRepositoryFactory implements Factory<SubscriptionAssignmentNotifyingRepository> {

    private final CuratorFramework curator;
    private final ConfigFactory configFactory;
    private final ZookeeperPaths zookeeperPaths;
    private final SubscriptionsCache subscriptionsCache;

    @Inject
    public SubscriptionAssignmentNotifyingRepositoryFactory(
            CuratorFramework curator,
            ConfigFactory configFactory,
            ZookeeperPaths zookeeperPaths,
            SubscriptionsCache subscriptionsCache
    ) {
        this.curator = curator;
        this.configFactory = configFactory;
        this.zookeeperPaths = zookeeperPaths;
        this.subscriptionsCache = subscriptionsCache;
    }

    @Override
    public SubscriptionAssignmentNotifyingRepository provide() {
        return new SubscriptionAssignmentCache(curator, configFactory, zookeeperPaths, subscriptionsCache);
    }

    @Override
    public void dispose(SubscriptionAssignmentNotifyingRepository instance) {

    }
}
