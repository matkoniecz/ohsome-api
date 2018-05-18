package org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.swagger.annotations.ApiModelProperty;

/**
 * Represents the whole JSON response object for the data aggregation response using the /ratio
 * resource. It contains the license and copyright, optional
 * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.Metadata
 * Metadata}, as well as the results section showing
 * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.RatioResult
 * RatioResult} objects.
 */
@JsonInclude(Include.NON_NULL)
public class RatioResponse {

  @ApiModelProperty(notes = "License and copyright info", required = true)
  private Attribution attribution;
  @ApiModelProperty(notes = "Version of this api", required = true)
  private String apiVersion;
  @ApiModelProperty(notes = "Metadata describing the output")
  private Metadata metadata;
  @ApiModelProperty(notes = "ElementsResult for /ratio requests", required = true)
  private RatioResult[] ratioResult;

  public RatioResponse(Attribution attribution, String apiVersion, Metadata metadata,
      RatioResult[] ratioResult) {
    this.attribution = attribution;
    this.apiVersion = apiVersion;
    this.metadata = metadata;
    this.ratioResult = ratioResult;
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

  public RatioResult[] getRatioResult() {
    return ratioResult;
  }
}