package org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation;

import org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.DefaultSwaggerParameters;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.ParameterDescriptions;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.exception.BadRequestException;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.executor.RequestParameters;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.executor.UsersRequestExecutor;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.DefaultAggregationResponse;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

/** REST controller containing the GET and POST requests, which enter through "/users". */
@Api(tags = "users")
@RestController
@RequestMapping("/users")
public class UsersController {

  /**
   * GET request giving the count of OSM users.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.DefaultAggregationResponse
   *         DefaultAggregationResponse}
   */
  @ApiOperation(value = "Count of OSM users", nickname = "getUsersCount")
  @RequestMapping(value = "/count", method = RequestMethod.GET, produces = "application/json")
  public DefaultAggregationResponse getCount(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCount(new RequestParameters(false, false, false, bboxes,
        bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * GET request giving the count of OSM users grouped by the OSM type.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Count of OSM users grouped by the type",
      nickname = "getUsersCountGroupByType")
  @RequestMapping(value = "/count/groupBy/type", method = RequestMethod.GET,
      produces = "application/json")
  public GroupByResponse getCountGroupByType(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByType(new RequestParameters(false, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * GET request giving the count of OSM users grouped by the tag.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Count of OSM users grouped by the tag",
      nickname = "getUsersCountGroupByTag")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "groupByKey", value = ParameterDescriptions.KEYS_DESCR,
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, paramType = "query",
          dataType = "string", required = true),
      @ApiImplicitParam(name = "groupByValues", value = ParameterDescriptions.VALUES_DESCR,
          defaultValue = "", paramType = "query", dataType = "string", required = false)})
  @RequestMapping(value = "count/groupBy/tag", method = RequestMethod.GET,
      produces = "application/json")
  public GroupByResponse getCountGroupByTag(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata,
      @RequestParam(value = "groupByKey", defaultValue = "", required = false) String[] groupByKey,
      @RequestParam(value = "groupByValues", defaultValue = "",
          required = false) String[] groupByValues)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByTag(new RequestParameters(false, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKey,
        groupByValues);
  }

  /**
   * GET request giving the count of OSM users grouped by the key.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Count of OSM users grouped by the tag",
      nickname = "getUsersCountGroupByKey")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "groupByKeys", value = ParameterDescriptions.KEYS_DESCR,
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, paramType = "query",
          dataType = "string", required = true)})
  @RequestMapping(value = "count/groupBy/key", method = RequestMethod.GET,
      produces = "application/json")
  public GroupByResponse getCountGroupByKey(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata,
      @RequestParam(value = "groupByKeys", defaultValue = "", required = false) String[] groupByKey)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByKey(new RequestParameters(false, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKey);
  }

  /**
   * GET request giving the density of OSM users (number of users per square-kilometers).
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.DefaultAggregationResponse
   *         DefaultAggregationResponse}
   */
  @ApiOperation(value = "Density of OSM users (number of users per square-kilometers)",
      nickname = "getUsersCountDensity")
  @RequestMapping(value = "/count/density", method = RequestMethod.GET,
      produces = "application/json")
  public DefaultAggregationResponse getCountDensity(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCount(new RequestParameters(false, false, true, bboxes,
        bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * GET request giving the density of OSM users grouped by the OSM type.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Density of OSM users grouped by the type",
      nickname = "getUsersCountDensityGroupByType")
  @RequestMapping(value = "/count/density/groupBy/type", method = RequestMethod.GET,
      produces = "application/json")
  public GroupByResponse getCountDensityGroupByType(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByType(new RequestParameters(false, false, true,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * GET request giving the density of OSM users grouped by the tag.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Density of OSM users grouped by the tag",
      nickname = "getUsersCountDensityGroupByTag")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "groupByKey", value = ParameterDescriptions.KEYS_DESCR,
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, paramType = "query",
          dataType = "string", required = true),
      @ApiImplicitParam(name = "groupByValues", value = ParameterDescriptions.VALUES_DESCR,
          defaultValue = "", paramType = "query", dataType = "string", required = false)})
  @RequestMapping(value = "/count/density/groupBy/tag", method = RequestMethod.GET,
      produces = "application/json")
  public GroupByResponse getCountDensityGroupByTag(
      @ApiParam(hidden = true) @RequestParam(value = "bboxes", defaultValue = "",
          required = false) String bboxes,
      @ApiParam(hidden = true) @RequestParam(value = "bcircles", defaultValue = "",
          required = false) String bcircles,
      @ApiParam(hidden = true) @RequestParam(value = "bpolys", defaultValue = "",
          required = false) String bpolys,
      @ApiParam(hidden = true) @RequestParam(value = "types", defaultValue = "",
          required = false) String[] types,
      @ApiParam(hidden = true) @RequestParam(value = "keys", defaultValue = "",
          required = false) String[] keys,
      @ApiParam(hidden = true) @RequestParam(value = "values", defaultValue = "",
          required = false) String[] values,
      @ApiParam(hidden = true) @RequestParam(value = "userids", defaultValue = "",
          required = false) String[] userids,
      @ApiParam(hidden = true) @RequestParam(value = "time", defaultValue = "",
          required = false) String[] time,
      @ApiParam(hidden = true) @RequestParam(value = "showMetadata",
          defaultValue = "false") String showMetadata,
      @RequestParam(value = "groupByKey", defaultValue = "", required = false) String[] groupByKey,
      @RequestParam(value = "groupByValues", defaultValue = "",
          required = false) String[] groupByValues)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByTag(new RequestParameters(false, false, true,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKey,
        groupByValues);
  }

  /**
   * POST request giving the count of OSM users. POST requests should only be used if the request
   * URL would be too long for a GET request.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.DefaultAggregationResponse
   *         DefaultAggregationResponse}
   */
  @ApiOperation(value = "Count of OSM users", nickname = "postUsersCount")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TYPE, required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = "building", required = false, value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string",
          defaultValue = "residential", required = false,
          value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR)})
  @RequestMapping(value = "/count", method = RequestMethod.POST, produces = "application/json",
      consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public DefaultAggregationResponse postCount(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata) throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCount(new RequestParameters(true, false, false, bboxes,
        bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * POST request giving the count of OSM users grouped by the OSM type. POST requests should only
   * be used if the request URL would be too long for a GET request.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Count of OSM users grouped by the type",
      nickname = "postUsersCountGroupByType")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = "way, relation", required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string", defaultValue = "",
          required = false, value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR)})
  @RequestMapping(value = "/count/groupBy/type", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public GroupByResponse postCountGroupByType(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata) throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByType(new RequestParameters(true, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * POST request giving the count of OSM users grouped by the tag. POST requests should only be
   * used if the request URL would be too long for a GET request.
   * <p>
   * The other parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @param groupByKey <code>String</code> array containing the key used to create the tags for the
   *        grouping. At the current implementation, there must be one key given (not more and not
   *        less).
   * @param groupByValues <code>String</code> array containing the values used to create the tags
   *        for grouping. If a given value does not appear in the output, then there are no objects
   *        assigned to it (within the given filters).
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponse Content}
   */
  @ApiOperation(value = "Count of OSM users grouped by the tag",
      nickname = "postUsersCountGroupByTag")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TYPE, required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string", defaultValue = "",
          required = false, value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR),
      @ApiImplicitParam(name = "groupByKey", paramType = "form", dataType = "string",
          defaultValue = "height", required = true, value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "groupByValues", paramType = "form", dataType = "string",
          defaultValue = "", required = false, value = ParameterDescriptions.VALUES_DESCR)})
  @RequestMapping(value = "/count/groupBy/tag", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public GroupByResponse postCountGroupByTag(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata, String[] groupByKey, String[] groupByValues)
      throws UnsupportedOperationException, Exception, BadRequestException {

    return UsersRequestExecutor.executeCountGroupByTag(new RequestParameters(true, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKey,
        groupByValues);
  }

  /**
   * POST request giving the count of OSM users grouped by the key. POST requests should only be
   * used if the request URL would be too long for a GET request.
   * <p>
   * The other parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @param groupByKeys <code>String</code> array containing the keys used for the grouping.
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponse Content}
   */
  @ApiOperation(value = "Count of OSM users grouped by the key",
      nickname = "postUsersCountGroupByKey")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TYPE, required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string", defaultValue = "",
          required = false, value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR),
      @ApiImplicitParam(name = "groupByKeys", paramType = "form", dataType = "string",
          defaultValue = "height", required = true, value = ParameterDescriptions.KEYS_DESCR)})
  @RequestMapping(value = "/count/groupBy/key", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public GroupByResponse postCountGroupByKey(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata, String[] groupByKeys)
      throws UnsupportedOperationException, Exception, BadRequestException {

    return UsersRequestExecutor.executeCountGroupByKey(new RequestParameters(true, false, false,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKeys);
  }

  /**
   * POST request giving the density of OSM users (number of users per square-kilometers). POST
   * requests should only be used if the request URL would be too long for a GET request.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.DefaultAggregationResponse
   *         DefaultAggregationResponse}
   */
  @ApiOperation(value = "Density of OSM users (number of users per square-kilometers)",
      nickname = "postUsersCountDensity")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TYPE, required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.RESIDENTIAL_VALUE, required = false,
          value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR)})
  @RequestMapping(value = "/count/density", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public DefaultAggregationResponse postCountDensity(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata) throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCount(new RequestParameters(true, false, true, bboxes,
        bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * POST request giving the density of OSM users grouped by the OSM type. POST requests should only
   * be used if the request URL would be too long for a GET request.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Density of OSM users grouped by the type",
      nickname = "postUsersCountDensityGroupByType")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = "way, relation", required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string", defaultValue = "",
          required = false, value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR)})
  @RequestMapping(value = "/count/density/groupBy/type", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public GroupByResponse postCountDensityGroupByType(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata) throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByType(new RequestParameters(true, false, true,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata));
  }

  /**
   * POST request giving the density of OSM users grouped by the tag. POST requests should only be
   * used if the request URL would be too long for a GET request.
   * <p>
   * The parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.dataAggregation.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.output.dataAggregationResponse.groupByResponse.GroupByResponse
   *         GroupByResponseContent}
   */
  @ApiOperation(value = "Density of OSM users grouped by the tag",
      nickname = "postUsersCountDensityGroupByTag")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "bboxes", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BBOX, required = false,
          value = ParameterDescriptions.BBOXES_DESCR),
      @ApiImplicitParam(name = "bcircles", paramType = "form", dataType = "string",
          required = false, value = ParameterDescriptions.BCIRCLES_DESCR),
      @ApiImplicitParam(name = "bpolys", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.BPOLYS_DESCR),
      @ApiImplicitParam(name = "types", paramType = "form", dataType = "string",
          defaultValue = "way, relation", required = false,
          value = ParameterDescriptions.TYPES_DESCR),
      @ApiImplicitParam(name = "keys", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.BUILDING_KEY, required = false,
          value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "values", paramType = "form", dataType = "string", defaultValue = "",
          required = false, value = ParameterDescriptions.VALUES_DESCR),
      @ApiImplicitParam(name = "userids", paramType = "form", dataType = "string", required = false,
          value = ParameterDescriptions.USERIDS_DESCR),
      @ApiImplicitParam(name = "time", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.TIME, required = false,
          value = ParameterDescriptions.TIME_DESCR),
      @ApiImplicitParam(name = "showMetadata", paramType = "form", dataType = "string",
          defaultValue = DefaultSwaggerParameters.SHOW_METADATA, required = false,
          value = ParameterDescriptions.SHOW_METADATA_DESCR),
      @ApiImplicitParam(name = "groupByKey", paramType = "form", dataType = "string",
          defaultValue = "height", required = true, value = ParameterDescriptions.KEYS_DESCR),
      @ApiImplicitParam(name = "groupByValues", paramType = "form", dataType = "string",
          defaultValue = "", required = false, value = ParameterDescriptions.VALUES_DESCR)})
  @RequestMapping(value = "/count/density/groupBy/tag", method = RequestMethod.POST,
      produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public GroupByResponse postCountDensityGroupByTag(String bboxes, String bcircles, String bpolys,
      String[] types, String[] keys, String[] values, String[] userids, String[] time,
      String showMetadata, String[] groupByKey, String[] groupByValues)
      throws UnsupportedOperationException, Exception {

    return UsersRequestExecutor.executeCountGroupByTag(new RequestParameters(true, false, true,
        bboxes, bcircles, bpolys, types, keys, values, userids, time, showMetadata), groupByKey,
        groupByValues);
  }

}
