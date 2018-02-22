package org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.metadata;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.swagger.annotations.ApiModelProperty;

/**
 * Represents the meta data JSON object for the /groupBy/boundary response containing the execution
 * time, the unit, the boundary array and a description of the values, which are in the
 * {@link org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.result.Result
 * Result} objects, as well as the request URL
 */
@JsonInclude(Include.NON_NULL)
public class GroupByBoundaryMetadata {

  @ApiModelProperty(notes = "Time the server needed to execute the request", required = true,
      position = 0)
  private long executionTime;
  @ApiModelProperty(notes = "Unit of the value in the result object(s)", required = true,
      position = 1)
  private String unit;
  @ApiModelProperty(
      notes = "Name of the boundary object & its coordinates (+ radius in case of bcircles)",
      required = true, position = 2)
  private Map<String, double[]> boundary;
  @ApiModelProperty(notes = "Text describing the result in a sentence", required = true,
      position = 3)
  private String description;
  @ApiModelProperty(notes = "Request URL to which this whole output JSON was generated",
      required = true, position = 4)
  private String requestURL;

  public GroupByBoundaryMetadata(long executionTime, String unit, Map<String, double[]> boundary,
      String description, String requestURL) {
    this.executionTime = executionTime;
    this.unit = unit;
    this.boundary = boundary;
    this.description = description;
    this.requestURL = requestURL;
  }

  public long getExecutionTime() {
    return executionTime;
  }

  public String getUnit() {
    return unit;
  }

  public Map<String, double[]> getBoundary() {
    return boundary;
  }

  public String getDescription() {
    return description;
  }

  public String getRequestURL() {
    return requestURL;
  }
}