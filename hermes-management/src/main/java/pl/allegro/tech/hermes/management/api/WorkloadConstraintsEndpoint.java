package pl.allegro.tech.hermes.management.api;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import org.springframework.stereotype.Component;
import pl.allegro.tech.hermes.api.SubscriptionConstraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicConstraints;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.domain.workload.constraints.ConsumersWorkloadConstraints;
import pl.allegro.tech.hermes.management.domain.workload.constraints.WorkloadConstraintsService;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Component
@Path("/workload-constraints")
@Api(value = "/workload-constraints", description = "Operations on workload constraints")
public class WorkloadConstraintsEndpoint {

    private final WorkloadConstraintsService service;

    public WorkloadConstraintsEndpoint(WorkloadConstraintsService service) {
        this.service = service;
    }

    @GET
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "All workload constraints", response = List.class, httpMethod = HttpMethod.GET)
    public ConsumersWorkloadConstraints getConsumersWorkloadConstraints() {
        return service.getConsumersWorkloadConstraints();
    }

    @POST
    @Path("/topic")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Create topic constraints", response = String.class, httpMethod = HttpMethod.POST)
    public Response createTopicConstraints(TopicConstraints topicConstraints) {
        service.createConstraints(topicConstraints.getTopicName(), topicConstraints.getConstraints());
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/topic/{topicName}")
    @ApiOperation(value = "Remove topic constraints", response = String.class, httpMethod = HttpMethod.DELETE)
    public Response deleteTopicConstraints(@PathParam("topicName") String topicName) {
        service.deleteConstraints(TopicName.fromQualifiedName(topicName));
        return Response.status(Response.Status.OK).build();
    }

    @POST
    @Path("/subscription")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Create subscription constraints", response = String.class, httpMethod = HttpMethod.POST)
    public Response createSubscriptionConstraints(SubscriptionConstraints subscriptionConstraints) {
        service.createConstraints(subscriptionConstraints.getSubscriptionName(), subscriptionConstraints.getConstraints());
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/subscription/{topicName}/{subscriptionName}")
    @ApiOperation(value = "Remove subscription constraints", response = String.class, httpMethod = HttpMethod.DELETE)
    public Response deleteSubscriptionConstraints(@PathParam("topicName") String topicName,
                                                  @PathParam("subscriptionName") String subscriptionName) {
        service.deleteConstraints(SubscriptionName.fromString(String.format("%s$%s", topicName, subscriptionName)));
        return Response.status(Response.Status.OK).build();
    }
}
