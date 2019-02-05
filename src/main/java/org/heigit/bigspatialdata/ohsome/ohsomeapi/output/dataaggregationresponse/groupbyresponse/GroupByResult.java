package org.heigit.bigspatialdata.ohsome.ohsomeapi.output.dataaggregationresponse.groupbyresponse;

import org.heigit.bigspatialdata.ohsome.ohsomeapi.output.dataaggregationresponse.Result;
import io.swagger.annotations.ApiModelProperty;

/**
 * Represents the groupBy result JSON object containing the groupBy value and the respective
 * {@link org.heigit.bigspatialdata.ohsome.ohsomeapi.output.dataaggregationresponse.Result Result}
 * objects. The GroupByResult is only used in responses for groupBy requests.
 */
public class GroupByResult extends GroupByObject{

  @ApiModelProperty(notes = "Result array holding timestamp-value pairs", required = true)
  private Result[] result;

  public GroupByResult(Object groupByName, Result[] result) {
    super(groupByName);
    this.result = result;
  }

  public Result[] getResult() {
    return result;
  }
}
