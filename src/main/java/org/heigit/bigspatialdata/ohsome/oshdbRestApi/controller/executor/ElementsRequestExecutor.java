package org.heigit.bigspatialdata.ohsome.oshdbRestApi.controller.executor;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.Application;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.exception.BadRequestException;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.inputValidation.InputValidator;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.interceptor.ElementsRequestInterceptor;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.Metadata;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.ElementsResponseContent;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.RatioResult;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.Result;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.ShareResult;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.groupByResponse.GroupByResponseContent;
import org.heigit.bigspatialdata.ohsome.oshdbRestApi.output.dataAggregationResponse.groupByResponse.GroupByResult;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBTimestampAndOtherIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapBiAggregatorByTimestamps;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.utils.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;

/**
 * Includes all execute methods for requests mapped to /elements.
 */
public class ElementsRequestExecutor {

  private final String license = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr,";
  private final String copyright =
      "sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua.";

  /**
   * Gets the input parameters of the request and performs a count calculation.
   */
  public ElementsResponseContent executeCount(boolean isPost, String bboxes, String bpoints,
      String bpolys, String[] types, String[] keys, String[] values, String[] userids,
      String[] time, String showMetadata) throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestamp, Integer> result;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp().count();
    // output
    Result[] resultSet = new Result[result.size()];
    int count = 0;
    for (Entry<OSHDBTimestamp, Integer> entry : result.entrySet()) {
      resultSet[count] =
          new Result(entry.getKey().formatIsoDateTime(), entry.getValue().intValue());
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount",
          "Total number of elements, which are selected by the parameters.", null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, resultSet, null, null);

    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count grouped by type.
   */
  public GroupByResponseContent executeCountGroupByType(boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<OSMType>, Integer> result;
    SortedMap<OSMType, SortedMap<OSHDBTimestamp, Integer>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .aggregateBy((SerializableFunction<OSMEntitySnapshot, OSMType>) f -> {
          return f.getEntity().getType();
        }).count();
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by user
    for (Entry<OSMType, SortedMap<OSHDBTimestamp, Integer>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Integer> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] =
            new Result(innerEntry.getKey().formatIsoDateTime(), innerEntry.getValue().intValue());
        innerCount++;
      }
      resultSet[count] = new GroupByResult(entry.getKey().toString(), results);
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount", "Total number of items aggregated on the type.",
          null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, resultSet, null, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count grouped by boundary.
   */
  public GroupByResponseContent executeCountGroupByBoundary(boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Integer>, Integer> result = null;
    SortedMap<Integer, SortedMap<OSHDBTimestamp, Integer>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // switch on the given boundary parameter
    switch (iV.getBoundary()) {
      case 0:
        throw new BadRequestException(
            "You need to give more than one bbox if you want to use /groupBy/boundary.");
      case 1:
        // create the geometry of the bboxes
        ArrayList<Geometry> bboxGeoms = iV.getGeometry("bbox");
        result = mapRed.aggregateByTimestamp().flatMap(f -> {
          List<Integer> bboxesList = new LinkedList<>();
          // check in which bbox the current element is (if any)
          for (int i = 0; i < bboxGeoms.size(); i++) {
            if (f.getGeometry().intersects(bboxGeoms.get(i))) {
              // add the ID of the current bbox to the list
              bboxesList.add(i);
            }
          }
          return bboxesList;
        }).aggregateBy(f -> f).count();
        break;
      case 2:
        // create the geometry of the bpoints
        ArrayList<Geometry> bpointGeoms = iV.getGeometry("bpoint");
        result = mapRed.aggregateByTimestamp().flatMap(f -> {
          List<Integer> bpointsList = new LinkedList<>();
          // check in which bpoint the current element is (if any)
          for (int i = 0; i < bpointGeoms.size(); i++) {
            if (f.getGeometry().intersects(bpointGeoms.get(i))) {
              // add the ID of the current bpoint to the list
              bpointsList.add(i);
            }
          }
          return bpointsList;
        }).aggregateBy(f -> f).count();
        break;
      case 3:
        // create the geometry of the bpolys
        ArrayList<Geometry> bpolyGeoms = iV.getGeometry("bpoly");
        result = mapRed.aggregateByTimestamp().flatMap(f -> {
          List<Integer> bpolysList = new LinkedList<>();
          // check in which bpoly the current element is (if any)
          for (int i = 0; i < bpolyGeoms.size(); i++) {
            if (f.getGeometry().intersects(bpolyGeoms.get(i))) {
              // add the ID of the current bpoly to the list
              bpolysList.add(i);
            }
          }
          return bpolysList;
        }).aggregateBy(f -> f).count();
        break;
    }
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    String groupByName = "";
    String[] boundaryIds = iV.getBoundaryIds();
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by the boundary
    for (Entry<Integer, SortedMap<OSHDBTimestamp, Integer>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // set the name of the current boundary object
      groupByName = boundaryIds[count];
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Integer> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] =
            new Result(innerEntry.getKey().formatIsoDateTime(), innerEntry.getValue().intValue());
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount",
          "Total number of items aggregated on the boundary object.", null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, resultSet, null, null, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count grouped by key.
   */
  public GroupByResponseContent executeCountGroupByKey(boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata, String[] groupByKeys)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Integer>, Integer> result;
    SortedMap<Integer, SortedMap<OSHDBTimestamp, Integer>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt = new Integer[groupByKeys.length];
    if (groupByKeys == null || groupByKeys.length == 0) {
      throw new BadRequestException(
          "You need to give one groupByKey parameters, if you want to use groupBy/key");
    }
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // get the integer values for the given keys
    for (int i = 0; i < groupByKeys.length; i++) {
      keysInt[i] = tt.key2Int(groupByKeys[i]);
    }
    // group by key logic
    result = mapRed.flatMap(f -> {
      List<Pair<Integer, OSMEntitySnapshot>> res = new LinkedList<>();
      int[] tags = f.getEntity().getTags();
      for (int i = 0; i < tags.length; i += 2) {
        int tagKeyId = tags[i];
        for (int key : keysInt) {
          // if key in input key list
          if (tagKeyId == key) {
            res.add(new ImmutablePair<>(tagKeyId, f));
          }
        }
      }
      if (res.size() == 0)
        res.add(new ImmutablePair<>(-1, f));
      return res;
    }).aggregateByTimestamp().aggregateBy(Pair::getKey).map(Pair::getValue).count();

    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);

    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    String groupByName = "";
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by keys
    for (Entry<Integer, SortedMap<OSHDBTimestamp, Integer>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // check for non-remainder objects (which do have the defined key)
      if (entry.getKey() != -1) {
        groupByName = tt.key2String(entry.getKey());
      } else {
        groupByName = "remainder";
      }
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Integer> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] =
            new Result(innerEntry.getKey().formatIsoDateTime(), innerEntry.getValue().intValue());
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount", "Total number of items aggregated on the key.",
          null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, resultSet, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count grouped by tag.
   */
  public GroupByResponseContent executeCountGroupByTag(boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata, String[] groupByKey,
      String[] groupByValues) throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<ImmutablePair<Integer, Integer>>, Integer> result;
    SortedMap<ImmutablePair<Integer, Integer>, SortedMap<OSHDBTimestamp, Integer>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // check the groupByKey and groupByValues parameters
    if (groupByKey == null || groupByKey.length == 0)
      throw new BadRequestException("There has to be one groupByKey value given.");
    if (groupByValues == null)
      groupByValues = new String[0];
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt = new Integer[groupByKey.length];
    Integer[] valuesInt = new Integer[groupByValues.length];
    // will hold those input groupByValues, which cannot be found in the OSM data
    Set<String> valuesSet = new HashSet<String>(Arrays.asList(groupByValues));
    boolean hasUnresolvableKey = false;
    if (groupByKey == null || groupByKey.length == 0) {
      throw new BadRequestException(
          "You need to give one groupByKey parameters, if you want to use groupBy/tag");
    }
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // get the integer values for the given keys
    for (int i = 0; i < groupByKey.length; i++) {
      keysInt[i] = tt.key2Int(groupByKey[i]);
      // if the given key cannot be resolved
      if (keysInt[i] == null) {
        keysInt[i] = -2;
        hasUnresolvableKey = true;
      }
      if (groupByValues != null) {
        // get the integer values for the given values
        for (int j = 0; j < groupByValues.length; j++) {
          valuesInt[j] = tt.tag2Int(groupByKey[i], groupByValues[j]).getValue();
          if (valuesInt[j] == null) {
            valuesInt[j] = -1;
          }
        }
      }
    }
    // group by tag logic
    result = mapRed.map(f -> {
      int[] tags = f.getEntity().getTags();
      for (int i = 0; i < tags.length; i += 2) {
        int tagKeyId = tags[i];
        int tagValueId = tags[i + 1];
        for (int key : keysInt) {
          // if key in input key list
          if (tagKeyId == key) {
            if (valuesInt.length == 0) {
              return new ImmutablePair<>(new ImmutablePair<Integer, Integer>(tagKeyId, tagValueId),
                  f);
            }
            for (int value : valuesInt) {
              // if value in input value list
              if (tagValueId == value)
                return new ImmutablePair<>(
                    new ImmutablePair<Integer, Integer>(tagKeyId, tagValueId), f);
            }
          }
        }
      }
      return new ImmutablePair<>(new ImmutablePair<Integer, Integer>(-1, -1), f);
    }).aggregateByTimestamp().aggregateBy(Pair::getKey).map(Pair::getValue).count();

    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size() + valuesSet.size() + 1];
    String groupByName = "";
    int count = 0;
    // needed in case of unresolved values
    int entrySetSize = 0;
    ArrayList<String> resultTimestamps = new ArrayList<String>();
    // iterate over the entry objects aggregated by tags
    for (Entry<ImmutablePair<Integer, Integer>, SortedMap<OSHDBTimestamp, Integer>> entry : groupByResult
        .entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      entrySetSize = entry.getValue().entrySet().size();
      int innerCount = 0;
      // check for non-remainder objects (which do have the defined key and value)
      if (entry.getKey().getKey() != -1 && entry.getKey().getValue() != -1) {
        groupByName = groupByKey[0] + "=" + tt.tag2String(entry.getKey()).getValue();
        valuesSet.remove(tt.tag2String(entry.getKey()).getValue());
      } else if (entry.getKey().getKey() == -2) {
        groupByName = "remainder";
      } else {
        groupByName = "remainder";
        // happens when the groupByKey is in keytables but not in the defined filter
        if (groupByResult.entrySet().size() == 1)
          hasUnresolvableKey = true;
      }
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Integer> innerEntry : entry.getValue().entrySet()) {
        if (count == 0)
          // write the timestamps into an ArrayList for later usage
          resultTimestamps.add(innerEntry.getKey().formatIsoDateTime());
        results[innerCount] =
            new Result(innerEntry.getKey().formatIsoDateTime(), innerEntry.getValue().intValue());
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    // if the groupByKey is unresolved
    if (hasUnresolvableKey) {
      Result[] results = new Result[entrySetSize];
      int innerCount = 0;
      for (String timestamp : resultTimestamps) {
        results[innerCount] = new Result(timestamp, 0.0);
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByKey[0], results);
    } else {
      // adding of the unresolved values
      if (valuesSet.size() > 0) {
        for (String value : valuesSet) {
          Result[] results = new Result[entrySetSize];
          int innerCount = 0;
          for (String timestamp : resultTimestamps) {
            results[innerCount] = new Result(timestamp, 0.0);
            innerCount++;
          }
          resultSet[count] = new GroupByResult(groupByKey[0] + "=" + value, results);
          count++;
        }
      }
    }
    // remove null objects in the resultSet
    resultSet = Arrays.stream(resultSet).filter(Objects::nonNull).toArray(GroupByResult[]::new);
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount", "Total number of items aggregated on the tag.",
          null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, null, resultSet, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count grouped by user.
   */
  public GroupByResponseContent executeCountGroupByUser(boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Integer>, Integer> result;
    SortedMap<Integer, SortedMap<OSHDBTimestamp, Integer>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .aggregateBy((SerializableFunction<OSMEntitySnapshot, Integer>) f -> {
          return f.getEntity().getUserId();
        }).count();
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by user
    for (Entry<Integer, SortedMap<OSHDBTimestamp, Integer>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Integer> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] =
            new Result(innerEntry.getKey().formatIsoDateTime(), innerEntry.getValue().intValue());
        innerCount++;
      }
      resultSet[count] = new GroupByResult(entry.getKey().toString(), results);
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount",
          "Total number of items aggregated on the userids.", null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, null, null, resultSet);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a count-share calculation.
   */
  public ElementsResponseContent executeCountShare(boolean isPost, String bboxes, String bpoints,
      String bpolys, String[] types, String[] keys, String[] values, String[] userids,
      String[] time, String showMetadata, String[] keys2, String[] values2)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Boolean>, Integer> result;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // check on null and length of keys2 and values 2
    if (keys2 == null || keys2.length < 1)
      throw new BadRequestException(
          "You need to define at least one key if you want to use /share.");
    if (values2 == null)
      values2 = new String[0];
    if (keys2.length < values2.length)
      throw new BadRequestException(
          "There cannot be more input values in values2 than in keys2 as values2n must fit to keys2n.");
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt2 = new Integer[keys2.length];
    Integer[] valuesInt2 = new Integer[values2.length];
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // get the integer values for the given keys
    for (int i = 0; i < keys2.length; i++) {
      keysInt2[i] = tt.key2Int(keys2[i]);
      if (keysInt2[i] == null)
        throw new BadRequestException(
            "All provided keys2 parameters have to be in the OSM database.");
      if (values2 != null && i < values2.length) {
        valuesInt2[i] = tt.tag2Int(keys2[i], values2[i]).getValue();
        if (valuesInt2[i] == null)
          throw new BadRequestException(
              "All provided values2 parameters have to fit to keys2 and be in the OSM database.");
      }
    }
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    result = mapRed.aggregateByTimestamp().aggregateBy(f -> {
      // result aggregated on true (if obj contains all tags) and false (if not all are contained)
      boolean hasTags = false;
      // if there is the same amount of keys and values
      for (int i = 0; i < keysInt2.length; i++) {
        if (f.getEntity().hasTagKey(keysInt2[i])) {
          if (i >= valuesInt2.length) {
            // if more keys2 than values2 are given
            hasTags = true;
            continue;
          }
          if (f.getEntity().hasTagValue(keysInt2[i], valuesInt2[i])) {
            hasTags = true;
          } else {
            hasTags = false;
            break;
          }
        } else {
          hasTags = false;
          break;
        }
      }
      return hasTags;
    }).count();

    Integer[] whole = new Integer[result.size()];
    Integer[] part = new Integer[result.size()];
    String[] timeArray = new String[result.size()];
    // needed time array in case no key can be found
    String[] noPartTimeArray = new String[result.size()];
    int partCount = 0;
    int wholeCount = 0;
    int timeCount = 0;
    // fill whole and part arrays with -1 values to indicate "no value"
    for (int i = 0; i < result.size(); i++) {
      whole[i] = -1;
      part[i] = -1;
    }
    // time and value extraction
    for (Entry<OSHDBTimestampAndOtherIndex<Boolean>, Integer> entry : result.entrySet()) {
      // this time array counts for each entry in the entrySet
      noPartTimeArray[timeCount] = entry.getKey().getTimeIndex().formatIsoDateTime();
      if (entry.getKey().getOtherIndex()) {
        // if true - set timestamp and set/increase part and/or whole
        timeArray[partCount] = entry.getKey().getTimeIndex().formatIsoDateTime();
        part[partCount] = entry.getValue();

        if (whole[partCount] == null || whole[partCount] == -1)
          whole[partCount] = entry.getValue();
        else
          whole[partCount] = whole[partCount] + entry.getValue();

        partCount++;
      } else {
        // else - set/increase only whole
        if (whole[wholeCount] == null || whole[wholeCount] == -1)
          whole[wholeCount] = entry.getValue();
        else
          whole[wholeCount] = whole[wholeCount] + entry.getValue();

        wholeCount++;
      }
      timeCount++;
    }
    // remove the possible null values in the array
    timeArray = Arrays.stream(timeArray).filter(Objects::nonNull).toArray(String[]::new);
    // overwrite time array in case the given key for part is not existent in the whole for no
    // timestamp
    if (timeArray.length < 1) {
      timeArray = noPartTimeArray;
    }
    // output
    ShareResult[] resultSet = new ShareResult[timeArray.length];
    for (int i = 0; i < timeArray.length; i++) {
      // set whole or part to 0 if they have -1 (== no value)
      if (whole[i] == -1)
        whole[i] = 0;
      if (part[i] == -1)
        part[i] = 0;

      resultSet[i] = new ShareResult(timeArray[i], whole[i], part[i]);
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount",
          "Share of items satisfying keys2 and values2 within items selected by types, keys, values.",
          null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, null, null, resultSet);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a length or area calculation.
   * 
   * @param isArea <code>Boolean</code> defining an area (true) or a length (false) request.
   */
  public ElementsResponseContent executeLengthArea(boolean isArea, boolean isPost, String bboxes,
      String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestamp, Number> result;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit;
    String description;
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;

    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          if (isArea) {
            return Geo.areaOf(snapshot.getGeometry());
          } else {
            return Geo.lengthOf(snapshot.getGeometry());
          }
        });
    // output
    Result[] resultSet = new Result[result.size()];
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    for (Map.Entry<OSHDBTimestamp, Number> entry : result.entrySet()) {
      resultSet[count] = new Result(entry.getKey().formatIsoDateTime(),
          Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue())));
      count++;
    }
    if (isArea) {
      unit = "square-meter";
      description = "Total area of polygons.";
    } else {
      unit = "meter";
      description = "Total length of lines.";
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, resultSet, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a perimeter calculation.
   */
  public ElementsResponseContent executePerimeter(boolean isPost, String bboxes, String bpoints,
      String bpolys, String[] types, String[] keys, String[] values, String[] userids,
      String[] time, String showMetadata) throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestamp, Number> result;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;

    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          // checks if the geometry is polygonal (needed for OSM relations, which are not polygonal)
          if (snapshot.getGeometry() instanceof Polygonal)
            return Geo.lengthOf(snapshot.getGeometry().getBoundary());
          else
            return 0.0;
        });
    // output
    Result[] resultSet = new Result[result.size()];
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    for (Map.Entry<OSHDBTimestamp, Number> entry : result.entrySet()) {
      resultSet[count] = new Result(entry.getKey().formatIsoDateTime(),
          Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue())));
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata =
          new Metadata(duration, "meters", "Total perimeter of polygonal items.", null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, resultSet, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and computes the length, perimeter, or area results
   * grouped by the key.
   * 
   * @param requestType <code>Byte</code> defining a length (1), perimeter (2), or area (3) request.
   */
  public GroupByResponseContent executeLengthPerimeterAreaGroupByKey(byte requestType,
      boolean isPost, String bboxes, String bpoints, String bpolys, String[] types, String[] keys,
      String[] values, String[] userids, String[] time, String showMetadata, String[] groupByKeys)
      throws UnsupportedOperationException, Exception {
    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Integer>, Number> result;
    SortedMap<Integer, SortedMap<OSHDBTimestamp, Number>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit = "";
    String description = "";
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt = new Integer[groupByKeys.length];
    if (groupByKeys == null || groupByKeys.length == 0) {
      throw new BadRequestException(
          "You need to give one groupByKey parameters, if you want to use groupBy/tag");
    }
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // get the integer values for the given keys
    for (int i = 0; i < groupByKeys.length; i++) {
      keysInt[i] = tt.key2Int(groupByKeys[i]);
    }
    // group by key logic
    result = mapRed.flatMap(f -> {
      List<Pair<Integer, OSMEntitySnapshot>> res = new LinkedList<>();
      int[] tags = f.getEntity().getTags();
      for (int i = 0; i < tags.length; i += 2) {
        int tagKeyId = tags[i];
        for (int key : keysInt) {
          // if key in input key list
          if (tagKeyId == key) {
            res.add(new ImmutablePair<>(tagKeyId, f));
          }
        }
      }
      if (res.size() == 0)
        res.add(new ImmutablePair<>(-1, f));
      return res;
    }).aggregateByTimestamp().aggregateBy(Pair::getKey).map(Pair::getValue)
        .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          switch (requestType) {
            case 1:
              return Geo.lengthOf(snapshot.getGeometry());
            case 2:
              if (snapshot.getGeometry() instanceof Polygonal)
                return Geo.lengthOf(snapshot.getGeometry().getBoundary());
              else
                return 0.0;
            case 3:
              return Geo.areaOf(snapshot.getGeometry());
            default:
              return 0.0;
          }
        });

    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);

    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    String groupByName = "";
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by keys
    for (Entry<Integer, SortedMap<OSHDBTimestamp, Number>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // check for non-remainder objects (which do have the defined key)
      if (entry.getKey() != -1) {
        groupByName = tt.key2String(entry.getKey());
      } else {
        groupByName = "remainder";
      }
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Number> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] = new Result(innerEntry.getKey().formatIsoDateTime(),
            Double.parseDouble(lengthPerimeterAreaDf.format(innerEntry.getValue().doubleValue())));
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    // setting of the unit and description output parameters
    switch (requestType) {
      case 1:
        unit = "meter";
        description = "Total length of items aggregated on the key.";
        break;
      case 2:
        unit = "meter";
        description = "Total perimeter of polygonal items aggregated on the key.";
        break;
      case 3:
        unit = "square-meter";
        description = "Total area of items aggregated on the key.";
        break;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, resultSet, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and computes the length, perimeter, or area results
   * grouped by the tag.
   * 
   * @param requestType <code>Byte</code> defining a length (1), perimeter (2), or area (3) request.
   */
  public GroupByResponseContent executeLengthPerimeterAreaGroupByTag(byte requestType,
      boolean isPost, String bboxes, String bpoints, String bpolys, String[] types, String[] keys,
      String[] values, String[] userids, String[] time, String showMetadata, String[] groupByKey,
      String[] groupByValues) throws UnsupportedOperationException, Exception {
    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<ImmutablePair<Integer, Integer>>, Number> result;
    SortedMap<ImmutablePair<Integer, Integer>, SortedMap<OSHDBTimestamp, Number>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit = "";
    String description = "";
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // check the groupByKey and groupByValues parameters
    if (groupByKey == null || groupByKey.length == 0)
      throw new BadRequestException(
          "You need to give one groupByKey parameters, if you want to use groupBy/tag.");
    if (groupByValues == null)
      groupByValues = new String[0];
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt = new Integer[groupByKey.length];
    Integer[] valuesInt = new Integer[groupByValues.length];
    // will hold those input groupByValues, which cannot be found in the OSM data
    Set<String> valuesSet = new HashSet<String>(Arrays.asList(groupByValues));
    boolean hasUnresolvableKey = false;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // get the integer values for the given keys
    for (int i = 0; i < groupByKey.length; i++) {
      keysInt[i] = tt.key2Int(groupByKey[i]);
      // if the given key cannot be resolved
      if (keysInt[i] == null) {
        keysInt[i] = -2;
        hasUnresolvableKey = true;
      }
      if (groupByValues != null) {
        // get the integer values for the given values
        for (int j = 0; j < groupByValues.length; j++) {
          // gets null, if value cannot be found in keytables
          valuesInt[j] = tt.tag2Int(groupByKey[i], groupByValues[j]).getValue();
          if (valuesInt[j] == null) {
            valuesInt[j] = -1;
          }
        }
      }
    }
    // group by tag logic
    result = mapRed.map(f -> {
      int[] tags = f.getEntity().getTags();
      for (int i = 0; i < tags.length; i += 2) {
        int tagKeyId = tags[i];
        int tagValueId = tags[i + 1];
        for (int key : keysInt) {
          // if key is unresolved
          if (key == -2) {
            return new ImmutablePair<>(new ImmutablePair<Integer, Integer>(-2, -1), f);
          }
          // if key in input key list
          if (tagKeyId == key) {
            if (valuesInt.length == 0) {
              return new ImmutablePair<>(new ImmutablePair<Integer, Integer>(tagKeyId, tagValueId),
                  f);
            }
            for (int j = 0; j < valuesInt.length; j++) {
              // if value in input value list
              if (tagValueId == valuesInt[j])
                return new ImmutablePair<>(
                    new ImmutablePair<Integer, Integer>(tagKeyId, tagValueId), f);
            }
          }
        }
      }
      return new ImmutablePair<>(new ImmutablePair<Integer, Integer>(-1, -1), f);
    }).aggregateByTimestamp().aggregateBy(Pair::getKey).map(Pair::getValue)
        .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          switch (requestType) {
            case 1:
              return Geo.lengthOf(snapshot.getGeometry());
            case 2:
              if (snapshot.getGeometry() instanceof Polygonal)
                return Geo.lengthOf(snapshot.getGeometry().getBoundary());
              else
                return 0.0;
            case 3:
              return Geo.areaOf(snapshot.getGeometry());
            default:
              return 0.0;
          }
        });
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    // +1 is needed in case the groupByKey is unresolved (not in keytables)
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size() + valuesSet.size() + 1];
    String groupByName = "";
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    // needed in case of unresolved values
    int entrySetSize = 0;
    ArrayList<String> resultTimestamps = new ArrayList<String>();
    // iterate over the entry objects aggregated by tags
    for (Entry<ImmutablePair<Integer, Integer>, SortedMap<OSHDBTimestamp, Number>> entry : groupByResult
        .entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      entrySetSize = entry.getValue().entrySet().size();
      int innerCount = 0;
      // check for non-remainder objects (which do have the defined key and value)
      if (entry.getKey().getKey() != -1 && entry.getKey().getValue() != -1) {
        groupByName = groupByKey[0] + "=" + tt.tag2String(entry.getKey()).getValue();
        valuesSet.remove(tt.tag2String(entry.getKey()).getValue());
      } else if (entry.getKey().getKey() == -2) {
        groupByName = "remainder";
      } else {
        groupByName = "remainder";
        // happens when the groupByKey is in keytables but not in the defined filter
        if (groupByResult.entrySet().size() == 1)
          hasUnresolvableKey = true;
      }
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Number> innerEntry : entry.getValue().entrySet()) {
        if (count == 0)
          // write the timestamps into an ArrayList for later usage
          resultTimestamps.add(innerEntry.getKey().formatIsoDateTime());
        results[innerCount] = new Result(innerEntry.getKey().formatIsoDateTime(),
            Double.parseDouble(lengthPerimeterAreaDf.format(innerEntry.getValue().doubleValue())));
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    // if the groupByKey is unresolved
    if (hasUnresolvableKey) {
      Result[] results = new Result[entrySetSize];
      int innerCount = 0;
      for (String timestamp : resultTimestamps) {
        results[innerCount] = new Result(timestamp, 0.0);
        innerCount++;
      }
      resultSet[count] = new GroupByResult(groupByKey[0], results);
    } else {
      // adding of the unresolved values
      if (valuesSet.size() > 0) {
        for (String value : valuesSet) {
          Result[] results = new Result[entrySetSize];
          int innerCount = 0;
          for (String timestamp : resultTimestamps) {
            results[innerCount] = new Result(timestamp, 0.0);
            innerCount++;
          }
          resultSet[count] = new GroupByResult(groupByKey[0] + "=" + value, results);
          count++;
        }
      }
    }
    // remove null objects in the resultSet
    resultSet = Arrays.stream(resultSet).filter(Objects::nonNull).toArray(GroupByResult[]::new);
    // setting of the unit and description output parameters
    switch (requestType) {
      case 1:
        unit = "meter";
        description = "Total length of items aggregated on the tag.";
        break;
      case 2:
        unit = "meter";
        description = "Total perimeter of polygonal items aggregated on the tag.";
        break;
      case 3:
        unit = "square-meter";
        description = "Total area of items aggregated on the tag.";
        break;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, null, resultSet, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and computes the length, perimeter, or area results
   * grouped by the user.
   * 
   * @param requestType <code>Byte</code> defining a length (1), perimeter (2), or area (3) request.
   */
  public GroupByResponseContent executeLengthPerimeterAreaGroupByUser(byte requestType,
      boolean isPost, String bboxes, String bpoints, String bpolys, String[] types, String[] keys,
      String[] values, String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Integer>, Number> result;
    SortedMap<Integer, SortedMap<OSHDBTimestamp, Number>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit = "";
    String description = "";
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .aggregateBy((SerializableFunction<OSMEntitySnapshot, Integer>) f -> {
          return f.getEntity().getUserId();
        }).sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          switch (requestType) {
            case 1:
              return Geo.lengthOf(snapshot.getGeometry());
            case 2:
              if (snapshot.getGeometry() instanceof Polygonal)
                return Geo.lengthOf(snapshot.getGeometry().getBoundary());
              else
                return 0.0;
            case 3:
              return Geo.areaOf(snapshot.getGeometry());
            default:
              return 0.0;
          }
        });
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by type
    for (Entry<Integer, SortedMap<OSHDBTimestamp, Number>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Number> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] = new Result(innerEntry.getKey().formatIsoDateTime(),
            Double.parseDouble(lengthPerimeterAreaDf.format(innerEntry.getValue().doubleValue())));
        innerCount++;
      }
      resultSet[count] = new GroupByResult(entry.getKey().toString(), results);
      count++;
    }
    // setting of the unit and description output parameters
    switch (requestType) {
      case 1:
        unit = "meter";
        description = "Total length of items aggregated on the userid.";
        break;
      case 2:
        unit = "meter";
        description = "Total perimeter of polygonal items aggregated on the userid.";
        break;
      case 3:
        unit = "square-meter";
        description = "Total area of items aggregated on the userid.";
        break;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, null, null, null, resultSet);
    return response;
  }

  /**
   * Gets the input parameters of the request and computes the area, or the perimeter grouped by the
   * OSM type.
   * 
   * @param isArea <code>Boolean</code> defining an area (true) or a length (false) request.
   */
  public GroupByResponseContent executeAreaPerimeterGroupByType(boolean isArea, boolean isPost,
      String bboxes, String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<OSMType>, Number> result;
    SortedMap<OSMType, SortedMap<OSHDBTimestamp, Number>> groupByResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit;
    String description;
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // db result
    result = mapRed.aggregateByTimestamp()
        .aggregateBy((SerializableFunction<OSMEntitySnapshot, OSMType>) f -> {
          return f.getEntity().getType();
        }).sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
          if (isArea) {
            return Geo.areaOf(snapshot.getGeometry());
          } else {
            if (snapshot.getGeometry() instanceof Polygonal)
              return Geo.lengthOf(snapshot.getGeometry().getBoundary());
            else
              return 0.0;
          }
        });
    groupByResult = MapBiAggregatorByTimestamps.nest_IndexThenTime(result);
    // output
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    int count = 0;
    int innerCount = 0;
    // iterate over the entry objects aggregated by type
    for (Entry<OSMType, SortedMap<OSHDBTimestamp, Number>> entry : groupByResult.entrySet()) {
      Result[] results = new Result[entry.getValue().entrySet().size()];
      innerCount = 0;
      // iterate over the inner entry objects containing timestamp-value pairs
      for (Entry<OSHDBTimestamp, Number> innerEntry : entry.getValue().entrySet()) {
        results[innerCount] = new Result(innerEntry.getKey().formatIsoDateTime(),
            Double.parseDouble(lengthPerimeterAreaDf.format(innerEntry.getValue().doubleValue())));
        innerCount++;
      }
      resultSet[count] = new GroupByResult(entry.getKey().toString(), results);
      count++;
    }
    // setting of the unit and description output parameters
    if (isArea) {
      unit = "square-meter";
      description = "Total area of items aggregated on the type.";
    } else {
      unit = "meter";
      description = "Total perimeter of items aggregated on the type.";
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    GroupByResponseContent response =
        new GroupByResponseContent(license, copyright, metadata, null, resultSet, null, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a density calculation.
   */
  public ElementsResponseContent executeDensity(boolean isPost, String bboxes, String bpoints,
      String bpolys, String[] types, String[] keys, String[] values, String[] userids,
      String[] time, String showMetadata) throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestamp, Integer> countResult;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    // count result
    countResult = mapRed.aggregateByTimestamp().count();
    int count = 0;
    Result[] countResultSet = new Result[countResult.size()];
    for (Entry<OSHDBTimestamp, Integer> entry : countResult.entrySet()) {
      countResultSet[count] =
          new Result(entry.getKey().formatIsoDateTime(), entry.getValue().intValue());
      count++;
    }
    // geometry
    Geometry geom = null;
    switch (iV.getBoundary()) {
      case 0:
        geom = iV.getBbox().getGeometry();
        break;
      case 1:
        geom = iV.getBbox().getGeometry();
        break;
      case 2:
        geom = iV.getBpointGeom();
        break;
      case 3:
        geom = iV.getBpoly();
        break;
    }
    // output
    Result[] resultSet = new Result[countResult.size()];
    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat densityDf = new DecimalFormat("#.##########", otherSymbols);
    for (int i = 0; i < resultSet.length; i++) {
      // gets the timestamp and the results from count and divides it through the area
      String date = countResultSet[i].getTimestamp();
      double value = Double.parseDouble(
          densityDf.format((countResultSet[i].getValue() / (Geo.areaOf(geom) / 1000000))));
      resultSet[i] = new Result(date, value);
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "items per square-kilometer",
          "Density of selected items (number of items per area).", null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, resultSet, null, null);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a length|perimeter|area-share
   * calculation.
   */
  public ElementsResponseContent executeLengthPerimeterAreaShare(byte requestType, boolean isPost,
      String bboxes, String bpoints, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata, String[] keys2, String[] values2)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestampAndOtherIndex<Boolean>, Number> result;
    MapReducer<OSMEntitySnapshot> mapRed;
    InputValidator iV = new InputValidator();
    String unit = "";
    String description = "";
    String requestURL = null;
    // check on null and length of keys2 and values 2
    if (keys2 == null || keys2.length < 1)
      throw new BadRequestException(
          "You need to define at least one key if you want to use /share.");
    if (values2 == null)
      values2 = new String[0];
    if (keys2.length < values2.length)
      throw new BadRequestException(
          "There cannot be more input values in values2 than in keys2 as values2n must fit to keys2n.");
    TagTranslator tt;
    // needed to get access to the keytables
    OSHDB_H2[] dbConnObjects = Application.getDbConnObjects();
    if (dbConnObjects[1] == null)
      tt = new TagTranslator(dbConnObjects[0].getConnection());
    else
      tt = new TagTranslator(dbConnObjects[1].getConnection());
    Integer[] keysInt2 = new Integer[keys2.length];
    Integer[] valuesInt2 = new Integer[values2.length];
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // get the integer values for the given keys
    for (int i = 0; i < keys2.length; i++) {
      keysInt2[i] = tt.key2Int(keys2[i]);
      if (keysInt2[i] == null)
        throw new BadRequestException(
            "All provided keys2 parameters have to be in the OSM database.");
      if (values2 != null && i < values2.length) {
        valuesInt2[i] = tt.tag2Int(keys2[i], values2[i]).getValue();
        if (valuesInt2[i] == null)
          throw new BadRequestException(
              "All provided values2 parameters have to fit to keys2 and be in the OSM database.");
      }
    }
    // input parameter processing
    mapRed = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    result = mapRed.aggregateByTimestamp().aggregateBy(f -> {
      // result aggregated on true (if obj contains all tags) and false (if not all are contained)
      boolean hasTags = false;
      // if there is the same amount of keys and values
      for (int i = 0; i < keysInt2.length; i++) {
        if (f.getEntity().hasTagKey(keysInt2[i])) {
          if (i >= valuesInt2.length) {
            // if more keys2 than values2 are given
            hasTags = true;
            continue;
          }
          if (f.getEntity().hasTagValue(keysInt2[i], valuesInt2[i])) {
            hasTags = true;
          } else {
            hasTags = false;
            break;
          }
        } else {
          hasTags = false;
          break;
        }
      }
      return hasTags;
    }).sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
      switch (requestType) {
        case 1:
          return Geo.lengthOf(snapshot.getGeometry());
        case 2:
          if (snapshot.getGeometry() instanceof Polygonal)
            return Geo.lengthOf(snapshot.getGeometry().getBoundary());
          else
            return 0.0;
        case 3:
          return Geo.areaOf(snapshot.getGeometry());
        default:
          return 0.0;
      }
    });

    DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.getDefault());
    otherSymbols.setDecimalSeparator('.');
    DecimalFormat lengthPerimeterAreaDf = new DecimalFormat("#.####", otherSymbols);
    Double[] whole = new Double[result.size()];
    Double[] part = new Double[result.size()];
    String[] timeArray = new String[result.size()];
    // needed time array in case no key can be found
    String[] noPartTimeArray = new String[result.size()];
    int partCount = 0;
    int wholeCount = 0;
    int timeCount = 0;
    // fill whole and part arrays with -1 values to indicate "no value"
    for (int i = 0; i < result.size(); i++) {
      whole[i] = -1.0;
      part[i] = -1.0;
    }
    // time and value extraction
    for (Entry<OSHDBTimestampAndOtherIndex<Boolean>, Number> entry : result.entrySet()) {
      // this time array counts for each entry in the entrySet
      noPartTimeArray[timeCount] = entry.getKey().getTimeIndex().formatIsoDateTime();
      if (entry.getKey().getOtherIndex()) {
        // if true - set timestamp and set/increase part and/or whole
        timeArray[partCount] = entry.getKey().getTimeIndex().formatIsoDateTime();
        part[partCount] =
            Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue()));

        if (whole[partCount] == null || whole[partCount] == -1)
          whole[partCount] =
              Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue()));
        else
          whole[partCount] = whole[partCount]
              + Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue()));

        partCount++;
      } else {
        // else - set/increase only whole
        if (whole[wholeCount] == null || whole[wholeCount] == -1)
          whole[wholeCount] =
              Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue()));
        else
          whole[wholeCount] = whole[partCount]
              + Double.parseDouble(lengthPerimeterAreaDf.format(entry.getValue().doubleValue()));

        wholeCount++;
      }
      timeCount++;
    }
    // remove the possible null values in the array
    timeArray = Arrays.stream(timeArray).filter(Objects::nonNull).toArray(String[]::new);
    // overwrite time array in case the given key for part is not existent in the whole for no
    // timestamp
    if (timeArray.length < 1) {
      timeArray = noPartTimeArray;
    }
    // output
    ShareResult[] resultSet = new ShareResult[timeArray.length];
    for (int i = 0; i < timeArray.length; i++) {
      // set whole or part to 0 if they have -1 (== no value)
      if (whole[i] == -1)
        whole[i] = 0.0;
      if (part[i] == -1)
        part[i] = 0.0;
      resultSet[i] = new ShareResult(timeArray[i], whole[i], part[i]);
    }
    // setting of the unit and description output parameters
    switch (requestType) {
      case 1:
        unit = "meter";
        description =
            "Total length of the whole and of a share of items satisfying keys2 and values2 within items selected by types, keys, values.";
        break;
      case 2:
        unit = "meter";
        description =
            "Total perimeter of the whole and of a share of items satisfying keys2 and values2 within items selected by types, keys, values.";
        break;
      case 3:
        unit = "square-meter";
        description =
            "Total area of the whole and of a share of items satisfying keys2 and values2 within items selected by types, keys, values.";
        break;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, unit, description, null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, null, null, resultSet);
    return response;
  }

  /**
   * Gets the input parameters of the request and performs a ratio calculation.
   */
  public ElementsResponseContent executeRatio(boolean isPost, String bboxes, String bpoints,
      String bpolys, String[] types, String[] keys, String[] values, String[] userids,
      String[] time, String showMetadata, String[] types2, String[] keys2, String[] values2)
      throws UnsupportedOperationException, Exception {

    long startTime = System.currentTimeMillis();
    SortedMap<OSHDBTimestamp, Integer> result1;
    SortedMap<OSHDBTimestamp, Integer> result2;
    MapReducer<OSMEntitySnapshot> mapRed1;
    MapReducer<OSMEntitySnapshot> mapRed2;
    InputValidator iV = new InputValidator();
    String requestURL = null;
    // request url is only returned in output for GET requests
    if (!isPost)
      requestURL = ElementsRequestInterceptor.requestUrl;
    // input parameter processing 1 and result 1
    mapRed1 = iV.processParameters(isPost, bboxes, bpoints, bpolys, types, keys, values, userids,
        time, showMetadata);
    result1 = mapRed1.aggregateByTimestamp().count();
    // input parameter processing 2 and result 2
    mapRed2 = iV.processParameters(isPost, bboxes, bpoints, bpolys, types2, keys2, values2, userids,
        time, showMetadata);
    result2 = mapRed2.aggregateByTimestamp().count();
    // resultSet 1
    Result[] resultSet1 = new Result[result1.size()];
    int count = 0;
    for (Entry<OSHDBTimestamp, Integer> entry : result1.entrySet()) {
      resultSet1[count] =
          new Result(entry.getKey().formatIsoDateTime(), entry.getValue().intValue());
      count++;
    }
    // output
    RatioResult[] resultSet = new RatioResult[result1.size()];
    count = 0;
    for (Entry<OSHDBTimestamp, Integer> entry : result2.entrySet()) {
      // gets the timestamp and the results from both counts and divides 2 through 1
      String date = resultSet1[count].getTimestamp();
      double ratio = entry.getValue().floatValue() / resultSet1[count].getValue();
      resultSet[count] =
          new RatioResult(date, resultSet1[count].getValue(), entry.getValue().intValue(), ratio);
      count++;
    }
    Metadata metadata = null;
    long duration = System.currentTimeMillis() - startTime;
    if (iV.getShowMetadata()) {
      metadata = new Metadata(duration, "amount and ratio",
          "Amount of items satisfying types2, keys2, values2 parameters (= value2 output) "
              + "within items selected by types, keys, values parameters (= value output) and ratio of value2:value.",
          null, requestURL);
    }
    // response
    ElementsResponseContent response =
        new ElementsResponseContent(license, copyright, metadata, null, null, resultSet, null);
    return response;
  }

}
