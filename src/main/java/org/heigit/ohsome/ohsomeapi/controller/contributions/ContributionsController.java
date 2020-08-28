package org.heigit.ohsome.ohsomeapi.controller.contributions;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.heigit.ohsome.ohsomeapi.controller.rawdata.ElementsGeometry;
import org.heigit.ohsome.ohsomeapi.executor.DataRequestExecutor;
import org.heigit.ohsome.ohsomeapi.executor.RequestResource;
import org.heigit.ohsome.ohsomeapi.output.rawdataresponse.DataResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller containing the methods, which are mapped to "/contributions" and used to return
 * each contribution (creation, modification, deletion) of the OSM data.
 */
@Api(tags = "Contributions")
@RestController
@RequestMapping("/contributions")
public class ContributionsController {

  /**
   * Gives the contributions as GeoJSON features, which have the geometry of the respective objects
   * in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(value = "OSM contributions having the raw geometry of each OSM object as geometry",
      nickname = "contributionsGeometry", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/geometry", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributions(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
      throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONS, ElementsGeometry.RAW, servletRequest, servletResponse);
  }

  /**
   * Gives the contributions as GeoJSON features, which have the bounding box of the respective
   * objects in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(value = "OSM contributions having the bounding box of each OSM object as geometry",
      nickname = "contributionsBbox", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/bbox", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributionsBbox(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONS, ElementsGeometry.BBOX, servletRequest, servletResponse);
  }

  /**
   * Gives the contributions as GeoJSON features, which have the centroid of the respective objects
   * in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(value = "OSM contributions having the centroid of each OSM object as geometry",
      nickname = "contributionsCentroid", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/centroid", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributionsCentroid(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONS, ElementsGeometry.CENTROID, servletRequest,
        servletResponse);
  }

  /**
   * Gives the latest contributions as GeoJSON features, which have the geometry of the respective
   * objects in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(
      value = "Latest OSM contributions having the raw geometry of each OSM object as geometry",
      nickname = "contributionsLatestGeometry", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/latest/geometry", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributionsLatest(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONSLATEST, ElementsGeometry.RAW,
        servletRequest, servletResponse);
  }

  /**
   * Gives the latest contributions as GeoJSON features, which have the bounding box of the
   * respective objects in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(
      value = "Latest OSM contributions having the bounding box of each OSM object as geometry",
      nickname = "contributionsLatestBbox", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/latest/bbox", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributionsBboxLatest(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONSLATEST, ElementsGeometry.BBOX,
        servletRequest, servletResponse);
  }

  /**
   * Gives the latest contributions as GeoJSON features, which have the centroid of the respective
   * objects in the geometry field.
   * 
   * @param servletRequest <code>HttpServletRequest</code> of the incoming request
   * @param servletResponse <code>HttpServletResponse</code> of the outgoing response
   */
  @ApiOperation(
      value = "Latest OSM contributions having the centroid of each OSM object as geometry",
      nickname = "contributionsLatestCentroid", response = DataResponse.class)
  @ApiImplicitParam(name = "time",
      value = "Two ISO-8601 conform timestrings defining an interval; no default value",
      defaultValue = "2016-01-01,2017-01-01", paramType = "query", dataType = "string",
      required = true)
  @RequestMapping(value = "/latest/centroid", method = {RequestMethod.GET, RequestMethod.POST},
      produces = "application/json")
  public void contributionsCentroidLatest(HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    DataRequestExecutor.extract(RequestResource.CONTRIBUTIONSLATEST, ElementsGeometry.CENTROID,
        servletRequest, servletResponse);
  }

}
