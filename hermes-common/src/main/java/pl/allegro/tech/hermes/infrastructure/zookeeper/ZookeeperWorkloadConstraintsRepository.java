package pl.allegro.tech.hermes.infrastructure.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.domain.workload.constraints.ConsumersWorkloadConstraints;
import pl.allegro.tech.hermes.domain.workload.constraints.SubscriptionConstraintsAlreadyExistException;
import pl.allegro.tech.hermes.domain.workload.constraints.TopicConstraintsAlreadyExistException;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraintsRepository;

public class ZookeeperWorkloadConstraintsRepository extends ZookeeperBasedRepository implements WorkloadConstraintsRepository {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWorkloadConstraintsRepository.class);

    private final ZookeeperWorkloadConstraintsCache pathChildrenCache;

    public ZookeeperWorkloadConstraintsRepository(CuratorFramework curator, ObjectMapper mapper, ZookeeperPaths paths) {
        this(curator, mapper, paths, new ZookeeperWorkloadConstraintsCache(curator, mapper, paths));
    }

    ZookeeperWorkloadConstraintsRepository(CuratorFramework curator, ObjectMapper mapper, ZookeeperPaths paths,
                                           ZookeeperWorkloadConstraintsCache pathChildrenCache) {
        super(curator, mapper, paths);
        this.pathChildrenCache = pathChildrenCache;
        try {
            this.pathChildrenCache.start();
        } catch (Exception e) {
            throw new InternalProcessingException("ZookeeperWorkloadConstraintsCache cannot start.", e);
        }
    }

    @Override
    public void createConstraints(TopicName topicName, Constraints constraints) {
        logger.info("Creating constraints for topic {}", topicName.qualifiedName());

        String path = paths.consumersWorkloadConstraintsPath(topicName.qualifiedName());
        try {
            createConstraints(path, constraints);
        } catch (KeeperException.NodeExistsException e) {
            throw new TopicConstraintsAlreadyExistException(topicName, e);
        }
    }

    @Override
    public void createConstraints(SubscriptionName subscriptionName, Constraints constraints) {
        logger.info("Creating constraints for subscription {}", subscriptionName.getQualifiedName());
        String path = paths.consumersWorkloadConstraintsPath(subscriptionName.getQualifiedName());
        try {
            createConstraints(path, constraints);
        } catch (KeeperException.NodeExistsException e) {
            throw new SubscriptionConstraintsAlreadyExistException(subscriptionName, e);
        }
    }

    private void createConstraints(String path, Constraints constraints) throws KeeperException.NodeExistsException {
        try {
            byte[] bytes = mapper.writeValueAsBytes(constraints);
            zookeeper.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, bytes);
        } catch (KeeperException.NodeExistsException e) {
            throw e;
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    @Override
    public ConsumersWorkloadConstraints getConsumersWorkloadConstraints() {
        return pathChildrenCache.getConsumersWorkloadConstraints();
    }

    @Override
    public void deleteConstraints(TopicName topicName) {
        String path = paths.consumersWorkloadConstraintsPath(topicName.qualifiedName());
        deleteConstraints(path);
    }

    @Override
    public void deleteConstraints(SubscriptionName subscriptionName) {
        String path = paths.consumersWorkloadConstraintsPath(subscriptionName.getQualifiedName());
        deleteConstraints(path);
    }

    private void deleteConstraints(String path) {
        try {
            zookeeper.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            // ignore - it's ok
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }
}
