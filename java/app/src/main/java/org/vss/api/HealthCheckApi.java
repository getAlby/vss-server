package org.vss.api;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.vss.GetObjectRequest;
import org.vss.GetObjectResponse;
import org.vss.KVStore;
import org.vss.auth.AuthResponse;
import org.vss.auth.Authorizer;

@Path(VssApiEndpoint.HEALTHCHECK)
@Slf4j
public class HealthCheckApi {
  KVStore kvStore;

  @Inject
  public HealthCheckApi(KVStore kvstore) {
    this.kvStore = kvstore;
  }

  @GET
  public Response execute(@Context HttpHeaders headers) {
    try {
      log.info("Healthcheck requested");
      kvStore.checkHealth();
      log.info("Healthcheck passed");
      
      return Response
        .status(Response.Status.OK)
        .build();
    } catch (Exception e) {
      log.error("Exception in HealthCheckApi: ", e);
      return Response.status(500)
        .entity("an unexpected error occurred: " + e.getMessage())
        .build();
    }
  }
}
