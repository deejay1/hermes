package pl.allegro.tech.hermes.domain.workload.constraints;

import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;

public interface WorkloadConstraintsRepository {

    void createConstraints(TopicName topicName, Constraints constraints);

    void createConstraints(SubscriptionName subscriptionName, Constraints constraints);

    ConsumersWorkloadConstraints getConsumersWorkloadConstraints();

    void deleteConstraints(TopicName topicName);

    void deleteConstraints(SubscriptionName subscriptionName);
}
