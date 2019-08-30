package pl.allegro.tech.hermes.infrastructure.zookeeper

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.zookeeper.KeeperException
import pl.allegro.tech.hermes.api.SubscriptionName
import pl.allegro.tech.hermes.api.TopicName
import pl.allegro.tech.hermes.api.Constraints
import pl.allegro.tech.hermes.domain.workload.constraints.SubscriptionConstraintsAlreadyExistException
import pl.allegro.tech.hermes.domain.workload.constraints.TopicConstraintsAlreadyExistException
import pl.allegro.tech.hermes.test.IntegrationTest

import java.util.concurrent.TimeUnit

import static com.jayway.awaitility.Awaitility.await

class ZookeeperWorkloadConstraintsRepositoryTest extends IntegrationTest {

    ZookeeperWorkloadConstraintsRepository repository
    ZookeeperWorkloadConstraintsCache cache
    def paths = new ZookeeperPaths("/hermes")
    def mapper = new ObjectMapper()

    def setup() {
        try {
            deleteAllNodes("/hermes/consumers-workload-constraints")
        } catch (Exception e) {
            e.printStackTrace()
        }

        cache = new ZookeeperWorkloadConstraintsCache(zookeeper(), mapper, paths)
        repository = new ZookeeperWorkloadConstraintsRepository(zookeeper(), mapper, paths, cache)
    }

    def cleanup() {
        cache.close()
    }

    def "should create constraints for topic"() {
        given:
        def topic = TopicName.fromQualifiedName('group.topic')

        when:
        repository.createConstraints(topic, new Constraints(1))
        wait.untilWorkloadConstraintsCreated(topic)

        then:
        assertNodeContainsData('/hermes/consumers-workload-constraints/group.topic', new Constraints(1))
    }

    def "should throw exception if topic node already exist"() {
        given:
        def topic = TopicName.fromQualifiedName('group.topic')

        when:
        repository.createConstraints(topic, new Constraints(1))
        wait.untilWorkloadConstraintsCreated(topic)
        repository.createConstraints(topic, new Constraints(1))

        then:
        def e = thrown(TopicConstraintsAlreadyExistException)
        e.message == "Constraints for topic group.topic already exist."
        e.cause instanceof KeeperException.NodeExistsException
    }

    def "should create constraints for subscription"() {
        given:
        def subscription = SubscriptionName.fromString('group.topic$sub')

        when:
        repository.createConstraints(subscription, new Constraints(1))
        wait.untilWorkloadConstraintsCreated(subscription)

        then:
        assertNodeContainsData('/hermes/consumers-workload-constraints/group.topic$sub', new Constraints(1))
    }

    def "should throw exception if subscription node already exist"() {
        given:
        def subscription = SubscriptionName.fromString('group.topic$sub')

        when:
        repository.createConstraints(subscription, new Constraints(1))
        wait.untilWorkloadConstraintsCreated(subscription)
        repository.createConstraints(subscription, new Constraints(1))

        then:
        def e = thrown(SubscriptionConstraintsAlreadyExistException)
        e.message == 'Constraints for subscription group.topic$sub already exist.'
        e.cause instanceof KeeperException.NodeExistsException
    }

    def "should return constraints from cache"() {
        given:
        setupNode('/hermes/consumers-workload-constraints/group.topic', new Constraints(1))
        setupNode('/hermes/consumers-workload-constraints/group.topic$sub', new Constraints(3))
        ensureCacheWasUpdated(2)

        when:
        def constraints = repository.getConsumersWorkloadConstraints()

        then:
        constraints.topicConstraints.get(TopicName.fromQualifiedName('group.topic')).consumersNumber == 1
        constraints.subscriptionConstraints.get(SubscriptionName.fromString('group.topic$sub')).consumersNumber == 3
    }

    def "should delete topic constraints"() {
        given:
        setupNode('/hermes/consumers-workload-constraints/group.topic', new Constraints(1))
        ensureCacheWasUpdated(1)

        when:
        repository.deleteConstraints(TopicName.fromQualifiedName('group.topic'))

        then:
        assertNodeDoesNotExists('/hermes/consumers-workload-constraints/group.topic')
    }

    def "should delete subscription constraints"() {
        given:
        setupNode('/hermes/consumers-workload-constraints/group.topic$sub', new Constraints(1))
        ensureCacheWasUpdated(1)

        when:
        repository.deleteConstraints(SubscriptionName.fromString('group.topic$sub'))

        then:
        assertNodeDoesNotExists('/hermes/consumers-workload-constraints/group.topic$sub')
    }

    def "should ignore not existing node"() {
        when:
        repository.deleteConstraints(TopicName.fromQualifiedName('group.topic'))

        then:
        noExceptionThrown()
    }

    private def ensureCacheWasUpdated(int expectedSize) {
        await()
                .atMost(200, TimeUnit.MILLISECONDS)
                .until { cache.getCurrentData().size() == expectedSize }
    }

    private def assertNodeContainsData(String path, Constraints constraints) {
        zookeeper().getData().forPath(path) == objectMapper.writeValueAsBytes(constraints)
    }

    private def assertNodeDoesNotExists(String path) {
        zookeeper().checkExists().forPath(path) == null
    }
}
