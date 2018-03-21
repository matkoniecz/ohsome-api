package org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse;

import org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.metadata.Metadata;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.result.UsersResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.swagger.annotations.ApiModelProperty;

@JsonInclude(Include.NON_NULL)
public class DefaultUsersResponse {

  @ApiModelProperty(notes = "License and copyright info", required = true, position = 0)
  private Attribution attribution;
  @ApiModelProperty(notes = "Version of this api", required = true, position = 1)
  private String apiVersion;
  @ApiModelProperty(notes = "Metadata describing the output", position = 2)
  private Metadata metadata;
  @ApiModelProperty(notes = "Size of the time interval in the results", required = true,
      position = 3)
  private String timeIntervalSize;
  @ApiModelProperty(notes = "Result holding from- and to timestamps plus the corresponding value",
      required = true)
  private UsersResult[] result;

  public DefaultUsersResponse(Attribution attribution, String apiVersion, Metadata metadata,
      String timeIntervalSize, UsersResult[] result) {
    this.attribution = attribution;
    this.apiVersion = apiVersion;
    this.metadata = metadata;
    this.timeIntervalSize = timeIntervalSize;
    this.result = result;
  }

  public Attribution getAttribution() {
    return attribution;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public String getTimeIntervalSize() {
    return timeIntervalSize;
  }

  public UsersResult[] getResult() {
    return result;
  }

}