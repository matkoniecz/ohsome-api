package org.heigit.ohsome.ohsomeapi.executor.dataaggregation;

import static org.heigit.ohsome.ohsomeapi.utils.GroupByBoundaryGeoJsonGenerator.createGeoJsonFeatures;

import com.opencsv.CSVWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.time.TimestampFormatter;
import org.heigit.ohsome.filter.FilterExpression;
import org.heigit.ohsome.filter.FilterParser;
import org.heigit.ohsome.ohsomeapi.Application;
import org.heigit.ohsome.ohsomeapi.exception.BadRequestException;
import org.heigit.ohsome.ohsomeapi.exception.ExceptionMessages;
import org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils;
import org.heigit.ohsome.ohsomeapi.executor.RequestExecutor;
import org.heigit.ohsome.ohsomeapi.executor.RequestParameters;
import org.heigit.ohsome.ohsomeapi.executor.RequestResource;
import org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils.MatchType;
import org.heigit.ohsome.ohsomeapi.inputprocessing.BoundaryType;
import org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessingUtils;
import org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor;
import org.heigit.ohsome.ohsomeapi.inputprocessing.ProcessingData;
import org.heigit.ohsome.ohsomeapi.inputprocessing.SimpleFeatureType;
import org.heigit.ohsome.ohsomeapi.oshdb.DbConnData;
import org.heigit.ohsome.ohsomeapi.oshdb.ExtractMetadata;
import org.heigit.ohsome.ohsomeapi.output.Attribution;
import org.heigit.ohsome.ohsomeapi.output.DefaultAggregationResponse;
import org.heigit.ohsome.ohsomeapi.output.Description;
import org.heigit.ohsome.ohsomeapi.output.Metadata;
import org.heigit.ohsome.ohsomeapi.output.Response;
import org.heigit.ohsome.ohsomeapi.output.contributions.ContributionsResult;
import org.heigit.ohsome.ohsomeapi.output.elements.ElementsResult;
import org.heigit.ohsome.ohsomeapi.output.groupby.GroupByObject;
import org.heigit.ohsome.ohsomeapi.output.groupby.GroupByResponse;
import org.heigit.ohsome.ohsomeapi.output.groupby.GroupByResult;
import org.heigit.ohsome.ohsomeapi.output.ratio.RatioResult;
import org.heigit.ohsome.ohsomeapi.utils.GroupByBoundaryGeoJsonGenerator;
import org.heigit.ohsome.ohsomeapi.utils.RequestUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

/**
 * Holds relevant execution methods for all elements aggregation requests.
 */
public class ElementsRequestExecutor extends RequestExecutor {

  public static final String URL = ExtractMetadata.attributionUrl;
  public static final String TEXT = ExtractMetadata.attributionShort;
  public static final DecimalFormat df = ExecutionUtils.defineDecimalFormat("#.##");
  private final RequestResource requestResource;
  private final InputProcessor inputProcessor;
  private final ProcessingData processingData;
  private final long startTime = System.currentTimeMillis();
  
  public ElementsRequestExecutor(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse, boolean isDensity) {
    super(servletRequest, servletResponse);
    this.requestResource = requestResource;
    inputProcessor = new InputProcessor(servletRequest, true, isDensity);
    processingData = inputProcessor.getProcessingData();
  }

  /**
   * Performs a count|length|perimeter|area calculation grouped by the boundary and the tag.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @param isSnapshot whether this request uses the snapshot-view (true), or contribution-view
   *        (false)
   * @param isDensity whether this request is accessed via the /density resource
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws BadRequestException if groupByKey parameter is not given
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters}
   */
  public static <P extends Geometry & Polygonal> Response aggregateGroupByBoundaryGroupByTag(
      RequestResource requestResource, HttpServletRequest servletRequest,
      HttpServletResponse servletResponse, boolean isSnapshot, boolean isDensity) throws Exception {
    final long startTime = System.currentTimeMillis();
    MapReducer<OSMEntitySnapshot> mapRed = null;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessor.getProcessingData().setGroupByBoundary(true);
    String[] groupByKey = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("groupByKey")));
    if (groupByKey.length != 1) {
      throw new BadRequestException(ExceptionMessages.GROUP_BY_KEY_PARAM);
    }
    mapRed = inputProcessor.processParameters();
    ProcessingData processingData = inputProcessor.getProcessingData();
    RequestParameters requestParameters = processingData.getRequestParameters();
    String[] groupByValues = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("groupByValues")));
    TagTranslator tt = DbConnData.tagTranslator;
    Integer[] valuesInt = new Integer[groupByValues.length];
    ArrayList<Pair<Integer, Integer>> zeroFill = new ArrayList<>();
    int keysInt = tt.getOSHDBTagKeyOf(groupByKey[0]).toInt();
    if (groupByValues.length != 0) {
      for (int j = 0; j < groupByValues.length; j++) {
        valuesInt[j] = tt.getOSHDBTagOf(groupByKey[0], groupByValues[j]).getValue();
        zeroFill.add(new ImmutablePair<>(keysInt, valuesInt[j]));
      }
    }
    var arrGeoms = new ArrayList<>(processingData.getBoundaryList());
    @SuppressWarnings("unchecked") // intentionally as check for P on Polygonal is already performed
    Map<Integer, P> geoms = IntStream.range(0, arrGeoms.size()).boxed()
        .collect(Collectors.toMap(idx -> idx, idx -> (P) arrGeoms.get(idx)));
    MapAggregator<Integer, OSMEntitySnapshot> mapAgg = mapRed.aggregateByGeometry(geoms);
    if (processingData.isContainingSimpleFeatureTypes()) {
      mapAgg = inputProcessor.filterOnSimpleFeatures(mapAgg);
    }
    Optional<FilterExpression> filter = processingData.getFilterExpression();
    if (filter.isPresent()) {
      mapAgg = mapAgg.filter(filter.get());
    }
    var preResult = mapAgg.map(f -> ExecutionUtils.mapSnapshotToTags(keysInt, valuesInt, f))
        .aggregateBy(Pair::getKey, zeroFill).map(Pair::getValue)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp).map(OSMEntitySnapshot::getGeometry);
    var result = ExecutionUtils.computeNestedResult(requestResource, preResult);
    var groupByResult = OSHDBCombinedIndex.nest(result);
    GroupByResult[] resultSet = new GroupByResult[groupByResult.entrySet().size()];
    InputProcessingUtils utils = inputProcessor.getUtils();
    Object[] boundaryIds = utils.getBoundaryIds();
    int count = 0;
    ArrayList<Geometry> boundaries = new ArrayList<>(processingData.getBoundaryList());
    for (var entry : groupByResult.entrySet()) {
      int boundaryIdentifier = entry.getKey().getFirstIndex();
      ElementsResult[] results = ExecutionUtils.fillElementsResult(
          entry.getValue(), requestParameters.isDensity(), df, boundaries.get(boundaryIdentifier));
      int tagValue = entry.getKey().getSecondIndex().getValue();
      String tagIdentifier;
      // check for non-remainder objects (which do have the defined key and value)
      if (entry.getKey().getSecondIndex().getKey() != -1 && tagValue != -1) {
        tagIdentifier = tt.getOSMTagOf(keysInt, tagValue).toString();
      } else {
        tagIdentifier = "remainder";
      }
      resultSet[count] =
          new GroupByResult(new Object[] {boundaryIds[boundaryIdentifier], tagIdentifier}, results);
      count++;
    }
    // used to remove null objects from the resultSet
    resultSet = Arrays.stream(resultSet).filter(Objects::nonNull).toArray(GroupByResult[]::new);
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      long duration = System.currentTimeMillis() - startTime;
      metadata = new Metadata(duration,
          Description.aggregateGroupByBoundaryGroupByTag(requestParameters.isDensity(),
              requestResource.getDescription(), requestResource.getUnit()),
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      ExecutionUtils exeUtils = new ExecutionUtils(processingData);
      exeUtils.writeCsvResponse(resultSet, servletResponse,
          ExecutionUtils.createCsvTopComments(URL, TEXT, Application.API_VERSION, metadata));
      return null;
    } else if ("geojson".equalsIgnoreCase(requestParameters.getFormat())) {
      return GroupByResponse.of(new Attribution(URL, TEXT), Application.API_VERSION, metadata,
          "FeatureCollection", GroupByBoundaryGeoJsonGenerator.createGeoJsonFeatures(resultSet,
              processingData.getGeoJsonGeoms()));
    }
    return new GroupByResponse(new Attribution(URL, TEXT), Application.API_VERSION, metadata,
        resultSet);
  }

  /**
   * Performs a count|length|perimeter|area calculation grouped by the tag.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @param isSnapshot whether this request uses the snapshot-view (true), or contribution-view
   *        (false)
   * @param isDensity whether this request is accessed via the /density resource
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws BadRequestException if groupByKey parameter is not given
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #computeResult(RequestResource, MapAggregator) computeResult}
   */
  public static Response aggregateGroupByTag(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse, boolean isSnapshot,
      boolean isDensity) throws Exception {
    final long startTime = System.currentTimeMillis();
    MapReducer<OSMEntitySnapshot> mapRed = null;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    String[] groupByKey = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("groupByKey")));
    if (groupByKey.length != 1) {
      throw new BadRequestException(ExceptionMessages.GROUP_BY_KEY_PARAM);
    }
    mapRed = inputProcessor.processParameters();
    ProcessingData processingData = inputProcessor.getProcessingData();
    RequestParameters requestParameters = processingData.getRequestParameters();
    String[] groupByValues = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("groupByValues")));
    TagTranslator tt = DbConnData.tagTranslator;
    Integer[] valuesInt = new Integer[groupByValues.length];
    ArrayList<Pair<Integer, Integer>> zeroFill = new ArrayList<>();
    int keysInt = tt.getOSHDBTagKeyOf(groupByKey[0]).toInt();
    if (groupByValues.length != 0) {
      for (int j = 0; j < groupByValues.length; j++) {
        valuesInt[j] = tt.getOSHDBTagOf(groupByKey[0], groupByValues[j]).getValue();
        zeroFill.add(new ImmutablePair<>(keysInt, valuesInt[j]));
      }
    }
    var preResult = mapRed.map(f -> ExecutionUtils.mapSnapshotToTags(keysInt, valuesInt, f))
        .aggregateByTimestamp().aggregateBy(Pair::getKey, zeroFill).map(Pair::getValue);
    var result = ExecutionUtils.computeResult(requestResource, preResult);
    var groupByResult = ExecutionUtils.nest(result);
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    String groupByName = "";
    Geometry geom = inputProcessor.getGeometry();
    int count = 0;
    for (var entry : groupByResult.entrySet()) {
      ElementsResult[] results = ExecutionUtils.fillElementsResult(
          entry.getValue(), requestParameters.isDensity(), df, geom);
      // check for non-remainder objects (which do have the defined key and value)
      if (entry.getKey().getKey() != -1 && entry.getKey().getValue() != -1) {
        groupByName = tt.getOSMTagOf(keysInt, entry.getKey().getValue()).toString();
      } else {
        groupByName = "remainder";
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    // used to remove null objects from the resultSet
    resultSet = Arrays.stream(resultSet).filter(Objects::nonNull).toArray(GroupByResult[]::new);
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      long duration = System.currentTimeMillis() - startTime;
      metadata = new Metadata(duration,
          Description.aggregateGroupByTag(requestParameters.isDensity(),
              requestResource.getDescription(), requestResource.getUnit()),
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      ExecutionUtils exeUtils = new ExecutionUtils(processingData);
      exeUtils.writeCsvResponse(resultSet, servletResponse,
          ExecutionUtils.createCsvTopComments(URL, TEXT, Application.API_VERSION, metadata));
      return null;
    }
    return new GroupByResponse(new Attribution(URL, TEXT), Application.API_VERSION, metadata,
        resultSet);
  }

  /**
   * Performs a count|length|perimeter|area calculation grouped by the OSM type.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @param isSnapshot whether this request uses the snapshot-view (true), or contribution-view
   *        (false)
   * @param isDensity whether this request is accessed via the /density resource
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #computeResult(RequestResource, MapAggregator) computeResult}
   */
  public static Response aggregateGroupByType(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse, boolean isSnapshot,
      boolean isDensity) throws Exception {
    final long startTime = System.currentTimeMillis();
    MapReducer<OSMEntitySnapshot> mapRed = null;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    mapRed = inputProcessor.processParameters();
    ProcessingData processingData = inputProcessor.getProcessingData();
    RequestParameters requestParameters = processingData.getRequestParameters();
    MapAggregator<OSHDBCombinedIndex<OSHDBTimestamp, OSMType>, OSMEntitySnapshot> preResult;
    preResult = mapRed.aggregateByTimestamp().aggregateBy(
        (SerializableFunction<OSMEntitySnapshot, OSMType>) f -> f.getEntity().getType(),
        processingData.getOsmTypes());
    var result = ExecutionUtils.computeResult(requestResource, preResult);
    var groupByResult = ExecutionUtils.nest(result);
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    Geometry geom = inputProcessor.getGeometry();
    int count = 0;
    for (var entry : groupByResult.entrySet()) {
      ElementsResult[] results = ExecutionUtils.fillElementsResult(
          entry.getValue(), requestParameters.isDensity(), df, geom);
      resultSet[count] = new GroupByResult(entry.getKey().toString(), results);
      count++;
    }
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      long duration = System.currentTimeMillis() - startTime;
      metadata = new Metadata(duration,
          Description.countPerimeterAreaGroupByType(requestParameters.isDensity(),
              requestResource.getDescription(), requestResource.getUnit()),
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      ExecutionUtils exeUtils = new ExecutionUtils(processingData);
      exeUtils.writeCsvResponse(resultSet, servletResponse,
          ExecutionUtils.createCsvTopComments(URL, TEXT, Application.API_VERSION, metadata));
      return null;
    }
    return new GroupByResponse(new Attribution(URL, TEXT), Application.API_VERSION, metadata,
        resultSet);
  }

  /**
   * Performs a count|length|perimeter|area calculation grouped by the key.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @param isSnapshot whether this request uses the snapshot-view (true), or contribution-view
   *        (false)
   * @param isDensity whether this request is accessed via the /density resource
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws BadRequestException if groupByKeys parameter is not given
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #computeResult(RequestResource, MapAggregator) computeResult}
   */
  public static Response aggregateGroupByKey(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse, boolean isSnapshot,
      boolean isDensity) throws Exception {
    final long startTime = System.currentTimeMillis();
    MapReducer<OSMEntitySnapshot> mapRed = null;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    String[] groupByKeys = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("groupByKeys")));
    if (groupByKeys == null || groupByKeys.length == 0) {
      throw new BadRequestException(ExceptionMessages.GROUP_BY_KEYS_PARAM);
    }
    mapRed = inputProcessor.processParameters();
    ProcessingData processingData = inputProcessor.getProcessingData();
    RequestParameters requestParameters = processingData.getRequestParameters();
    TagTranslator tt = DbConnData.tagTranslator;
    Integer[] keysInt = new Integer[groupByKeys.length];
    for (int i = 0; i < groupByKeys.length; i++) {
      keysInt[i] = tt.getOSHDBTagKeyOf(groupByKeys[i]).toInt();
    }
    MapAggregator<OSHDBCombinedIndex<OSHDBTimestamp, Integer>, OSMEntitySnapshot> preResult =
        mapRed.flatMap(f -> {
          List<Pair<Integer, OSMEntitySnapshot>> res = new LinkedList<>();
          Iterable<OSHDBTag> tags = f.getEntity().getTags();
          for (OSHDBTag tag : tags) {
            int tagKeyId = tag.getKey();
            for (int key : keysInt) {
              if (tagKeyId == key) {
                res.add(new ImmutablePair<>(tagKeyId, f));
              }
            }
          }
          if (res.isEmpty()) {
            res.add(new ImmutablePair<>(-1, f));
          }
          return res;
        }).aggregateByTimestamp().aggregateBy(Pair::getKey, Arrays.asList(keysInt))
            .map(Pair::getValue);
    var result = ExecutionUtils.computeResult(requestResource, preResult);
    var groupByResult = ExecutionUtils.nest(result);
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    String groupByName = "";
    int count = 0;
    for (var entry : groupByResult.entrySet()) {
      ElementsResult[] results = ExecutionUtils.fillElementsResult(
          entry.getValue(), requestParameters.isDensity(), df, null);
      // check for non-remainder objects (which do have the defined key)
      if (entry.getKey() != -1) {
        groupByName = tt.getOSMTagKeyOf(entry.getKey().intValue()).toString();
      } else {
        groupByName = "remainder";
      }
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      long duration = System.currentTimeMillis() - startTime;
      metadata =
          new Metadata(duration,
              Description.aggregateGroupByKey(requestResource.getDescription(),
                  requestResource.getUnit()),
              inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      ExecutionUtils exeUtils = new ExecutionUtils(processingData);
      exeUtils.writeCsvResponse(resultSet, servletResponse,
          ExecutionUtils.createCsvTopComments(URL, TEXT, Application.API_VERSION, metadata));
      return null;
    }
    return new GroupByResponse(new Attribution(URL, TEXT), Application.API_VERSION, metadata,
        resultSet);
  }

  /**
   * Performs a count|length|perimeter|area|ratio calculation.
   *
   * @deprecated Will be removed in next major version update.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #computeResult(RequestResource, MapAggregator) computeResult}
   */
  @Deprecated(forRemoval = true)
  public static Response aggregateBasicFiltersRatio(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws Exception {
    final long startTime = System.currentTimeMillis();
    // these 2 parameters always have these values for /ratio requests
    final boolean isSnapshot = true;
    final boolean isDensity = false;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessor.getProcessingData().setRatio(true);
    final MapReducer<OSMEntitySnapshot> intermediateMapRed = inputProcessor.processParameters();
    final ProcessingData processingData = inputProcessor.getProcessingData();
    final RequestParameters requestParameters = processingData.getRequestParameters();
    TagTranslator tt = DbConnData.tagTranslator;
    String[] keys2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("keys2")));
    String[] values2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("values2")));
    inputProcessor.checkKeysValues(keys2, values2);
    Pair<String[], String[]> keys2Vals2 = inputProcessor.processKeys2Vals2(keys2, values2);
    keys2 = keys2Vals2.getKey();
    values2 = keys2Vals2.getValue();
    Integer[] keysInt1 = new Integer[requestParameters.getKeys().length];
    Integer[] valuesInt1 = new Integer[requestParameters.getValues().length];
    Integer[] keysInt2 = new Integer[keys2.length];
    Integer[] valuesInt2 = new Integer[values2.length];
    for (int i = 0; i < requestParameters.getKeys().length; i++) {
      keysInt1[i] = tt.getOSHDBTagKeyOf(requestParameters.getKeys()[i]).toInt();
      if (requestParameters.getValues() != null && i < requestParameters.getValues().length) {
        valuesInt1[i] =
            tt.getOSHDBTagOf(requestParameters.getKeys()[i], requestParameters.getValues()[i])
                .getValue();
      }
    }
    for (int i = 0; i < keys2.length; i++) {
      keysInt2[i] = tt.getOSHDBTagKeyOf(keys2[i]).toInt();
      if (i < values2.length) {
        valuesInt2[i] = tt.getOSHDBTagOf(keys2[i], values2[i]).getValue();
      }
    }
    EnumSet<OSMType> osmTypes1 =
        (EnumSet<OSMType>) inputProcessor.getProcessingData().getOsmTypes();
    String[] types1 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("types")));
    String[] types2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("types2")));
    final EnumSet<SimpleFeatureType> simpleFeatureTypes1 =
        inputProcessor.defineSimpleFeatureTypes(types1);
    inputProcessor.defineTypes(types2, intermediateMapRed);
    EnumSet<OSMType> osmTypes2 =
        (EnumSet<OSMType>) inputProcessor.getProcessingData().getOsmTypes();
    final EnumSet<SimpleFeatureType> simpleFeatureTypes2 =
        inputProcessor.defineSimpleFeatureTypes(types2);
    EnumSet<OSMType> osmTypes = osmTypes1.clone();
    osmTypes.addAll(osmTypes2);
    String[] osmTypesString =
        osmTypes.stream().map(OSMType::toString).map(String::toLowerCase).toArray(String[]::new);
    MapReducer<OSMEntitySnapshot> mapRed = null;
    if (!inputProcessor.compareKeysValues(requestParameters.getKeys(), keys2,
        requestParameters.getValues(), values2)) {
      RequestParameters requestParams =
          new RequestParameters(servletRequest.getMethod(), isSnapshot, isDensity,
              servletRequest.getParameter("bboxes"), servletRequest.getParameter("bcircles"),
              servletRequest.getParameter("bpolys"), osmTypesString, new String[] {},
              new String[] {}, servletRequest.getParameterValues("time"),
              servletRequest.getParameter("format"), servletRequest.getParameter("showMetadata"),
              ProcessingData.getTimeout(), servletRequest.getParameter("filter"));
      ProcessingData secondProcessingData =
          new ProcessingData(requestParams, servletRequest.getRequestURL().toString());
      InputProcessor secondInputProcessor =
          new InputProcessor(servletRequest, isSnapshot, isDensity);
      secondInputProcessor.setProcessingData(secondProcessingData);
      mapRed = secondInputProcessor.processParameters();
    } else {
      mapRed = inputProcessor.processParameters();
    }
    mapRed = mapRed.osmType(osmTypes);
    mapRed = ExecutionUtils.snapshotFilter(mapRed, osmTypes1, osmTypes2, simpleFeatureTypes1,
        simpleFeatureTypes2, keysInt1, keysInt2, valuesInt1, valuesInt2);
    var preResult = mapRed.aggregateByTimestamp().aggregateBy(snapshot -> {
      boolean matches1 = ExecutionUtils.snapshotMatches(snapshot, osmTypes1,
          simpleFeatureTypes1, keysInt1, valuesInt1);
      boolean matches2 = ExecutionUtils.snapshotMatches(snapshot, osmTypes2,
          simpleFeatureTypes2, keysInt2, valuesInt2);
      if (matches1 && matches2) {
        return MatchType.MATCHESBOTH;
      } else if (matches1) {
        return MatchType.MATCHES1;
      } else if (matches2) {
        return MatchType.MATCHES2;
      } else {
        // this should never be reached
        assert false : "MatchType matches none.";
        return MatchType.MATCHESNONE;
      }
    }, EnumSet.allOf(MatchType.class));
    var result = ExecutionUtils.computeResult(requestResource, preResult);
    int resultSize = result.size();
    Double[] value1 = new Double[resultSize / 4];
    Double[] value2 = new Double[resultSize / 4];
    String[] timeArray = new String[resultSize / 4];
    int value1Count = 0;
    int value2Count = 0;
    int matchesBothCount = 0;
    // time and value extraction
    for (var entry : result.entrySet()) {
      if (entry.getKey().getSecondIndex() == MatchType.MATCHES2) {
        timeArray[value2Count] =
            TimestampFormatter.getInstance().isoDateTime(entry.getKey().getFirstIndex());
        value2[value2Count] = Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value2Count++;
      }
      if (entry.getKey().getSecondIndex() == MatchType.MATCHES1) {
        value1[value1Count] = Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value1Count++;
      }
      if (entry.getKey().getSecondIndex() == MatchType.MATCHESBOTH) {
        value1[matchesBothCount] = value1[matchesBothCount]
            + Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value2[matchesBothCount] = value2[matchesBothCount]
            + Double.parseDouble(df.format(entry.getValue().doubleValue()));
        matchesBothCount++;
      }
    }
    ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    return exeUtils.createRatioResponse(timeArray, value1, value2, startTime, requestResource,
        inputProcessor.getRequestUrlIfGetRequest(servletRequest), servletResponse);
  }

  /**
   * Performs a count|length|perimeter|area|ratio calculation using the filter and filter2
   * parameters.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #computeResult(RequestResource, MapAggregator) computeResult}
   */
  public static Response aggregateRatio(RequestResource requestResource,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws Exception {
    if (null == servletRequest.getParameter("filter")
        && (null != servletRequest.getParameter("types")
            || null != servletRequest.getParameter("keys"))) {
      return aggregateBasicFiltersRatio(requestResource, servletRequest, servletResponse);
    }
    final long startTime = System.currentTimeMillis();
    // these 2 parameters always have these values for /ratio requests
    final boolean isSnapshot = true;
    final boolean isDensity = false;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessor.getProcessingData().setRatio(true);
    inputProcessor.processParameters();
    final ProcessingData processingData = inputProcessor.getProcessingData();
    String filter1 = inputProcessor.getProcessingData().getRequestParameters().getFilter();
    String filter2 = inputProcessor.createEmptyStringIfNull(servletRequest.getParameter("filter2"));
    inputProcessor.checkFilter(filter2);
    String combinedFilter = ExecutionUtils.combineFiltersWithOr(filter1, filter2);
    FilterParser fp = new FilterParser(DbConnData.tagTranslator);
    FilterExpression filterExpr1 = inputProcessor.getUtils().parseFilter(fp, filter1);
    FilterExpression filterExpr2 = inputProcessor.getUtils().parseFilter(fp, filter2);
    RequestParameters requestParamsCombined = new RequestParameters(servletRequest.getMethod(),
        isSnapshot, isDensity, servletRequest.getParameter("bboxes"),
        servletRequest.getParameter("bcircles"), servletRequest.getParameter("bpolys"),
        new String[] {}, new String[] {}, new String[] {},
        servletRequest.getParameterValues("time"), servletRequest.getParameter("format"),
        servletRequest.getParameter("showMetadata"), ProcessingData.getTimeout(), combinedFilter);
    ProcessingData processingDataCombined =
        new ProcessingData(requestParamsCombined, servletRequest.getRequestURL().toString());
    InputProcessor inputProcessorCombined =
        new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessorCombined.setProcessingData(processingDataCombined);
    MapReducer<OSMEntitySnapshot> mapRed = inputProcessorCombined.processParameters();
    mapRed = mapRed.filter(combinedFilter);
    var preResult = mapRed.aggregateByTimestamp().aggregateBy(snapshot -> {
      OSMEntity entity = snapshot.getEntity();
      boolean matches1 = filterExpr1.applyOSMGeometry(entity, snapshot.getGeometry());
      boolean matches2 = filterExpr2.applyOSMGeometry(entity, snapshot.getGeometry());
      if (matches1 && matches2) {
        return MatchType.MATCHESBOTH;
      } else if (matches1) {
        return MatchType.MATCHES1;
      } else if (matches2) {
        return MatchType.MATCHES2;
      } else {
        // this should never be reached
        assert false : "MatchType matches none.";
        return MatchType.MATCHESNONE;
      }
    }, EnumSet.allOf(MatchType.class));
    var result = ExecutionUtils.computeResult(requestResource, preResult);
    int resultSize = result.size();
    int matchTypeSize = 4;
    Double[] value1 = new Double[resultSize / matchTypeSize];
    Double[] value2 = new Double[resultSize / matchTypeSize];
    String[] timeArray = new String[resultSize / matchTypeSize];
    int value1Count = 0;
    int value2Count = 0;
    int matchesBothCount = 0;
    // time and value extraction
    for (var entry : result.entrySet()) {
      if (entry.getKey().getSecondIndex() == MatchType.MATCHES2) {
        timeArray[value2Count] =
            TimestampFormatter.getInstance().isoDateTime(entry.getKey().getFirstIndex());
        value2[value2Count] = Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value2Count++;
      }
      if (entry.getKey().getSecondIndex() == MatchType.MATCHES1) {
        value1[value1Count] = Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value1Count++;
      }
      if (entry.getKey().getSecondIndex() == MatchType.MATCHESBOTH) {
        value1[matchesBothCount] = value1[matchesBothCount]
            + Double.parseDouble(df.format(entry.getValue().doubleValue()));
        value2[matchesBothCount] = value2[matchesBothCount]
            + Double.parseDouble(df.format(entry.getValue().doubleValue()));
        matchesBothCount++;
      }
    }
    ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    return exeUtils.createRatioResponse(timeArray, value1, value2, startTime, requestResource,
        inputProcessor.getRequestUrlIfGetRequest(servletRequest), servletResponse);
  }

  /**
   * Performs a count|length|perimeter|area-ratio calculation grouped by the boundary.
   *
   * @deprecated Will be removed in next major version update.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws BadRequestException if a boundary parameter (bboxes, bcircles, bpolys) is not defined
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters},
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#count() count}, or
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#sum() sum}
   */
  @Deprecated(forRemoval = true)
  public static <P extends Geometry & Polygonal> Response aggregateBasicFiltersRatioGroupByBoundary(
      RequestResource requestResource, HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    final long startTime = System.currentTimeMillis();
    final boolean isSnapshot = true;
    final boolean isDensity = false;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessor.getProcessingData().setGroupByBoundary(true);
    inputProcessor.getProcessingData().setRatio(true);
    final MapReducer<OSMEntitySnapshot> intermediateMapRed = inputProcessor.processParameters();
    final ProcessingData processingData = inputProcessor.getProcessingData();
    final RequestParameters requestParameters = processingData.getRequestParameters();
    if (processingData.getBoundaryType() == BoundaryType.NOBOUNDARY) {
      throw new BadRequestException(ExceptionMessages.NO_BOUNDARY);
    }
    String[] keys2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("keys2")));
    String[] values2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("values2")));
    inputProcessor.checkKeysValues(keys2, values2);
    Pair<String[], String[]> keys2Vals2 = inputProcessor.processKeys2Vals2(keys2, values2);
    keys2 = keys2Vals2.getKey();
    values2 = keys2Vals2.getValue();
    Integer[] keysInt1 = new Integer[requestParameters.getKeys().length];
    Integer[] valuesInt1 = new Integer[requestParameters.getValues().length];
    Integer[] keysInt2 = new Integer[keys2.length];
    Integer[] valuesInt2 = new Integer[values2.length];
    TagTranslator tt = DbConnData.tagTranslator;
    for (int i = 0; i < requestParameters.getKeys().length; i++) {
      keysInt1[i] = tt.getOSHDBTagKeyOf(requestParameters.getKeys()[i]).toInt();
      if (requestParameters.getValues() != null && i < requestParameters.getValues().length) {
        valuesInt1[i] =
            tt.getOSHDBTagOf(requestParameters.getKeys()[i], requestParameters.getValues()[i])
                .getValue();
      }
    }
    for (int i = 0; i < keys2.length; i++) {
      keysInt2[i] = tt.getOSHDBTagKeyOf(keys2[i]).toInt();
      if (i < values2.length) {
        valuesInt2[i] = tt.getOSHDBTagOf(keys2[i], values2[i]).getValue();
      }
    }
    EnumSet<OSMType> osmTypes1 = (EnumSet<OSMType>) processingData.getOsmTypes();
    String[] types1 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("types")));
    String[] types2 = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("types2")));
    final EnumSet<SimpleFeatureType> simpleFeatureTypes1 =
        inputProcessor.defineSimpleFeatureTypes(types1);
    inputProcessor.defineTypes(types2, intermediateMapRed);
    EnumSet<OSMType> osmTypes2 =
        (EnumSet<OSMType>) inputProcessor.getProcessingData().getOsmTypes();
    EnumSet<OSMType> osmTypes = osmTypes1.clone();
    final EnumSet<SimpleFeatureType> simpleFeatureTypes2 =
        inputProcessor.defineSimpleFeatureTypes(types2);
    osmTypes.addAll(osmTypes2);
    String[] osmTypesString =
        osmTypes.stream().map(OSMType::toString).map(String::toLowerCase).toArray(String[]::new);
    MapReducer<OSMEntitySnapshot> mapRed = null;
    if (!inputProcessor.compareKeysValues(requestParameters.getKeys(), keys2,
        requestParameters.getValues(), values2)) {
      RequestParameters requestParams =
          new RequestParameters(servletRequest.getMethod(), isSnapshot, isDensity,
              servletRequest.getParameter("bboxes"), servletRequest.getParameter("bcircles"),
              servletRequest.getParameter("bpolys"), osmTypesString, new String[] {},
              new String[] {}, servletRequest.getParameterValues("time"),
              servletRequest.getParameter("format"), servletRequest.getParameter("showMetadata"),
              ProcessingData.getTimeout(), servletRequest.getParameter("filter"));
      ProcessingData secondProcessingData =
          new ProcessingData(requestParams, servletRequest.getRequestURL().toString());
      InputProcessor secondInputProcessor =
          new InputProcessor(servletRequest, isSnapshot, isDensity);
      secondInputProcessor.setProcessingData(secondProcessingData);
      mapRed = secondInputProcessor.processParameters();
    } else {
      mapRed = inputProcessor.processParameters();
    }
    mapRed = mapRed.osmType(osmTypes);
    ArrayList<Geometry> arrGeoms = new ArrayList<>(processingData.getBoundaryList());
    // intentionally as check for P on Polygonal is already performed
    @SuppressWarnings({"unchecked"})
    Map<Integer, P> geoms =
        arrGeoms.stream().collect(Collectors.toMap(arrGeoms::indexOf, geom -> (P) geom));
    var mapRed2 = mapRed.aggregateByTimestamp().aggregateByGeometry(geoms);
    mapRed2 = ExecutionUtils.snapshotFilter(mapRed2, osmTypes1, osmTypes2, simpleFeatureTypes1,
        simpleFeatureTypes2, keysInt1, keysInt2, valuesInt1, valuesInt2);
    var preResult =
        mapRed2.aggregateBy((SerializableFunction<OSMEntitySnapshot, MatchType>) snapshot -> {
          boolean matches1 = ExecutionUtils.snapshotMatches(snapshot,
              osmTypes1, simpleFeatureTypes1, keysInt1, valuesInt1);
          boolean matches2 = ExecutionUtils.snapshotMatches(snapshot,
              osmTypes2, simpleFeatureTypes2, keysInt2, valuesInt2);
          if (matches1 && matches2) {
            return MatchType.MATCHESBOTH;
          } else if (matches1) {
            return MatchType.MATCHES1;
          } else if (matches2) {
            return MatchType.MATCHES2;
          } else {
            assert false : "MatchType matches none.";
          }
          return MatchType.MATCHESNONE;
        }, EnumSet.allOf(MatchType.class)).map(OSMEntitySnapshot::getGeometry);
    SortedMap<OSHDBCombinedIndex<OSHDBCombinedIndex<OSHDBTimestamp, Integer>, MatchType>, ? extends
        Number> result = null;
    switch (requestResource) {
      case COUNT:
        result = preResult.count();
        break;
      case LENGTH:
        result =
            preResult.sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom)));
        break;
      case PERIMETER:
        result = preResult.sum(geom -> {
          if (!(geom instanceof Polygonal)) {
            return 0.0;
          }
          return ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom.getBoundary()));
        });
        break;
      case AREA:
        result =
            preResult.sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.areaOf(geom)));
        break;
      default:
        break;
    }
    InputProcessingUtils utils = inputProcessor.getUtils();
    var groupByResult = ExecutionUtils.nest(result);
    Object[] boundaryIds = utils.getBoundaryIds();
    Double[] resultValues1 = null;
    Double[] resultValues2 = null;
    String[] timeArray = null;
    boolean timeArrayFilled = false;
    for (var entry : groupByResult.entrySet()) {
      var resultSet = entry.getValue().entrySet();
      if (!timeArrayFilled) {
        timeArray = new String[resultSet.size()];
      }
      if (entry.getKey() == MatchType.MATCHES2) {
        resultValues2 = ExecutionUtils.fillElementsRatioGroupByBoundaryResultValues(resultSet, df);
      } else if (entry.getKey() == MatchType.MATCHES1) {
        resultValues1 = ExecutionUtils.fillElementsRatioGroupByBoundaryResultValues(resultSet, df);
      } else if (entry.getKey() == MatchType.MATCHESBOTH) {
        int matchesBothCount = 0;
        int timeArrayCount = 0;
        for (var innerEntry : resultSet) {
          resultValues1[matchesBothCount] = resultValues1[matchesBothCount]
              + Double.parseDouble(df.format(innerEntry.getValue().doubleValue()));
          resultValues2[matchesBothCount] = resultValues2[matchesBothCount]
              + Double.parseDouble(df.format(innerEntry.getValue().doubleValue()));
          if (!timeArrayFilled) {
            String time = innerEntry.getKey().getFirstIndex().toString();
            if (matchesBothCount == 0 || !timeArray[timeArrayCount - 1].equals(time)) {
              timeArray[timeArrayCount] = innerEntry.getKey().getFirstIndex().toString();
              timeArrayCount++;
            }
          }
          matchesBothCount++;
        }
        timeArray = Arrays.stream(timeArray).filter(Objects::nonNull).toArray(String[]::new);
        timeArrayFilled = true;
      } else {
        // on MatchType.MATCHESNONE aggregated values are not needed / do not exist
      }
    }
    ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    return exeUtils.createRatioGroupByBoundaryResponse(boundaryIds, timeArray, resultValues1,
        resultValues2, startTime, requestResource,
        inputProcessor.getRequestUrlIfGetRequest(servletRequest), servletResponse);
  }

  /**
   * Performs a count|length|perimeter|area-ratio calculation grouped by the boundary using the
   * filter and filter2 parameters.
   *
   * @param requestResource {@link org.heigit.ohsome.ohsomeapi.executor.RequestResource
   *        RequestResource} definition of the request resource
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws BadRequestException if a boundary parameter (bboxes, bcircles, bpolys) is not defined
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters},
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#count() count}, or
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#sum() sum}
   */
  public static <P extends Geometry & Polygonal> Response aggregateRatioGroupByBoundary(
      RequestResource requestResource, HttpServletRequest servletRequest,
      HttpServletResponse servletResponse) throws Exception {
    if (null == servletRequest.getParameter("filter")
        && (null != servletRequest.getParameter("types")
            || null != servletRequest.getParameter("keys"))) {
      return aggregateBasicFiltersRatioGroupByBoundary(requestResource, servletRequest,
          servletResponse);
    }
    final long startTime = System.currentTimeMillis();
    // these 2 parameters always have these values for /ratio requests
    final boolean isSnapshot = true;
    final boolean isDensity = false;
    InputProcessor inputProcessor = new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessor.getProcessingData().setGroupByBoundary(true);
    inputProcessor.getProcessingData().setRatio(true);
    inputProcessor.processParameters();
    ProcessingData processingData = inputProcessor.getProcessingData();
    if (processingData.getBoundaryType() == BoundaryType.NOBOUNDARY) {
      throw new BadRequestException(ExceptionMessages.NO_BOUNDARY);
    }
    final String filter1 = inputProcessor.getProcessingData().getRequestParameters().getFilter();
    final String filter2 =
        inputProcessor.createEmptyStringIfNull(servletRequest.getParameter("filter2"));
    inputProcessor.checkFilter(filter2);
    final String combinedFilter = ExecutionUtils.combineFiltersWithOr(filter1, filter2);
    final FilterParser fp = new FilterParser(DbConnData.tagTranslator);
    final FilterExpression filterExpr1 = inputProcessor.getUtils().parseFilter(fp, filter1);
    final FilterExpression filterExpr2 = inputProcessor.getUtils().parseFilter(fp, filter2);
    RequestParameters requestParamsCombined = new RequestParameters(servletRequest.getMethod(),
        isSnapshot, isDensity, servletRequest.getParameter("bboxes"),
        servletRequest.getParameter("bcircles"), servletRequest.getParameter("bpolys"),
        new String[] {}, new String[] {}, new String[] {},
        servletRequest.getParameterValues("time"), servletRequest.getParameter("format"),
        servletRequest.getParameter("showMetadata"), ProcessingData.getTimeout(), combinedFilter);
    ProcessingData processingDataCombined =
        new ProcessingData(requestParamsCombined, servletRequest.getRequestURL().toString());
    InputProcessor inputProcessorCombined =
        new InputProcessor(servletRequest, isSnapshot, isDensity);
    inputProcessorCombined.setProcessingData(processingDataCombined);
    inputProcessorCombined.getProcessingData().setRatio(true);
    inputProcessorCombined.getProcessingData().setGroupByBoundary(true);
    MapReducer<OSMEntitySnapshot> mapRed = inputProcessorCombined.processParameters();
    ArrayList<Geometry> arrGeoms = new ArrayList<>(processingData.getBoundaryList());
    // intentionally as check for P on Polygonal is already performed
    @SuppressWarnings({"unchecked"})
    Map<Integer, P> geoms =
        arrGeoms.stream().collect(Collectors.toMap(arrGeoms::indexOf, geom -> (P) geom));
    var mapRed2 = mapRed.aggregateByTimestamp().aggregateByGeometry(geoms);
    mapRed2 = mapRed2.filter(combinedFilter);
    var preResult =
        mapRed2.aggregateBy((SerializableFunction<OSMEntitySnapshot, MatchType>) snapshot -> {
          OSMEntity entity = snapshot.getEntity();
          boolean matches1 = filterExpr1.applyOSMGeometry(entity, snapshot.getGeometry());
          boolean matches2 = filterExpr2.applyOSMGeometry(entity, snapshot.getGeometry());
          if (matches1 && matches2) {
            return MatchType.MATCHESBOTH;
          } else if (matches1) {
            return MatchType.MATCHES1;
          } else if (matches2) {
            return MatchType.MATCHES2;
          } else {
            assert false : "MatchType matches none.";
          }
          return MatchType.MATCHESNONE;
        }, EnumSet.allOf(MatchType.class)).map(OSMEntitySnapshot::getGeometry);
    SortedMap<OSHDBCombinedIndex<OSHDBCombinedIndex<OSHDBTimestamp, Integer>, MatchType>, ? extends
        Number> result = null;
    switch (requestResource) {
      case COUNT:
        result = preResult.count();
        break;
      case LENGTH:
        result =
            preResult.sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom)));
        break;
      case PERIMETER:
        result = preResult.sum(geom -> {
          if (!(geom instanceof Polygonal)) {
            return 0.0;
          }
          return ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom.getBoundary()));
        });
        break;
      case AREA:
        result =
            preResult.sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.areaOf(geom)));
        break;
      default:
        break;
    }
    InputProcessingUtils utils = inputProcessor.getUtils();
    var groupByResult = ExecutionUtils.nest(result);
    Object[] boundaryIds = utils.getBoundaryIds();
    Double[] resultValues1 = null;
    Double[] resultValues2 = null;
    String[] timeArray = null;
    boolean timeArrayFilled = false;
    for (var entry : groupByResult.entrySet()) {
      var resultSet = entry.getValue().entrySet();
      if (!timeArrayFilled) {
        timeArray = new String[resultSet.size()];
      }
      if (entry.getKey() == MatchType.MATCHES2) {
        resultValues2 = ExecutionUtils.fillElementsRatioGroupByBoundaryResultValues(resultSet, df);
      } else if (entry.getKey() == MatchType.MATCHES1) {
        resultValues1 = ExecutionUtils.fillElementsRatioGroupByBoundaryResultValues(resultSet, df);
      } else if (entry.getKey() == MatchType.MATCHESBOTH) {
        int matchesBothCount = 0;
        int timeArrayCount = 0;
        for (var innerEntry : resultSet) {
          resultValues1[matchesBothCount] = resultValues1[matchesBothCount]
              + Double.parseDouble(df.format(innerEntry.getValue().doubleValue()));
          resultValues2[matchesBothCount] = resultValues2[matchesBothCount]
              + Double.parseDouble(df.format(innerEntry.getValue().doubleValue()));
          if (!timeArrayFilled) {
            String time = innerEntry.getKey().getFirstIndex().toString();
            if (matchesBothCount == 0 || !timeArray[timeArrayCount - 1].equals(time)) {
              timeArray[timeArrayCount] = innerEntry.getKey().getFirstIndex().toString();
              timeArrayCount++;
            }
          }
          matchesBothCount++;
        }
        timeArray = Arrays.stream(timeArray).filter(Objects::nonNull).toArray(String[]::new);
        timeArrayFilled = true;
      } else {
        // on MatchType.MATCHESNONE aggregated values are not needed / do not exist
      }
    }
    ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    return exeUtils.createRatioGroupByBoundaryResponse(boundaryIds, timeArray, resultValues1,
        resultValues2, startTime, requestResource,
        inputProcessor.getRequestUrlIfGetRequest(servletRequest), servletResponse);
  }
  
  /**
   * Performs a count|length|perimeter|area calculation.
   *
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws RuntimeException if an unsupported RequestResource type is used. Only COUNT, LENGTH,
   *         PERIMETER, and AREA are permitted here
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters},
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#count() count}, or
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator#sum() sum}
   */
  public Response aggregate() throws Exception {
    final SortedMap<OSHDBTimestamp, ? extends Number> result;
    MapReducer<OSMEntitySnapshot> mapRed = null;
    mapRed = inputProcessor.processParameters();
    switch (requestResource) {
      case COUNT:
        result = mapRed.aggregateByTimestamp().count();
        break;
      case AREA:
        result = mapRed.aggregateByTimestamp()
            .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> ExecutionUtils
                .cacheInUserData(snapshot.getGeometry(), () -> Geo.areaOf(snapshot.getGeometry())));
        break;
      case LENGTH:
        result = mapRed.aggregateByTimestamp()
            .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> ExecutionUtils
                .cacheInUserData(snapshot.getGeometry(),
                    () -> Geo.lengthOf(snapshot.getGeometry())));
        break;
      case PERIMETER:
        result = mapRed.aggregateByTimestamp()
            .sum((SerializableFunction<OSMEntitySnapshot, Number>) snapshot -> {
              if (snapshot.getGeometry() instanceof Polygonal) {
                return ExecutionUtils.cacheInUserData(snapshot.getGeometry(),
                    () -> Geo.lengthOf(snapshot.getGeometry().getBoundary()));
              } else {
                return 0.0;
              }
            });
        break;
      default:
        throw new RuntimeException("Unsupported RequestResource type for this processing. "
            + "Only COUNT, LENGTH, PERIMETER, and AREA are permitted here");
    }
    Geometry geom = inputProcessor.getGeometry();
    RequestParameters requestParameters = processingData.getRequestParameters();
    ElementsResult[] resultSet =
        fillElementsResult(result, requestParameters.isDensity(), df, geom);
    String description = Description.aggregate(requestParameters.isDensity(),
        requestResource.getDescription(), requestResource.getUnit());
    Metadata metadata = generateMetadata(description);
    if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      return writeCsv(createCsvTopComments(metadata), writeCsvResponse(resultSet));
    }
    return DefaultAggregationResponse.of(ATTRIBUTION, Application.API_VERSION, metadata, resultSet);
  }

  /**
   * Performs a count|length|perimeter|area calculation grouped by the boundary.
   *
   * @return {@link org.heigit.ohsome.ohsomeapi.output.Response Response}
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters} and
   *         {@link org.heigit.ohsome.ohsomeapi.executor.AggregateRequestExecutor
   *         #computeCountLengthPerimeterAreaGbB(RequestResource, BoundaryType, MapReducer,
   *         InputProcessor) computeCountLengthPerimeterAreaGbB}
   */
  public Response aggregateGroupByBoundary() throws Exception {
    processingData.setGroupByBoundary(true);
    RequestParameters requestParameters = processingData.getRequestParameters();
    MapReducer<OSMEntitySnapshot> mapRed = inputProcessor.processParameters();
    InputProcessingUtils utils = inputProcessor.getUtils();
    var result = computeCountLengthPerimeterAreaGbB(requestResource,
        processingData.getBoundaryType(), mapRed, inputProcessor);
    SortedMap<Integer, ? extends SortedMap<OSHDBTimestamp, ? extends Number>> groupByResult;
    groupByResult = ExecutionUtils.nest(result);
    GroupByResult[] resultSet = new GroupByResult[groupByResult.size()];
    Object groupByName;
    Object[] boundaryIds = utils.getBoundaryIds();
    int count = 0;
    ArrayList<Geometry> boundaries = new ArrayList<>(processingData.getBoundaryList());
    for (Entry<Integer, ? extends SortedMap<OSHDBTimestamp, ? extends Number>> entry : groupByResult
        .entrySet()) {
      ElementsResult[] results = fillElementsResult(entry.getValue(), requestParameters.isDensity(),
          df, boundaries.get(count));
      groupByName = boundaryIds[count];
      resultSet[count] = new GroupByResult(groupByName, results);
      count++;
    }
    String description = Description.aggregate(requestParameters.isDensity(),
        requestResource.getDescription(), requestResource.getUnit());
    Metadata metadata = generateMetadata(description);
    if ("geojson".equalsIgnoreCase(requestParameters.getFormat())) {
      return GroupByResponse.of(ATTRIBUTION, Application.API_VERSION, metadata, "FeatureCollection",
          createGeoJsonFeatures(resultSet, processingData.getGeoJsonGeoms()));
    } else if ("csv".equalsIgnoreCase(requestParameters.getFormat())) {
      return writeCsv(createCsvTopComments(metadata), writeCsvResponse(resultSet));
    }
    return new GroupByResponse(ATTRIBUTION, Application.API_VERSION, metadata, resultSet);
  }

  /**
   * Creates the metadata for the JSON response containing info like execution time, request URL and
   * a short description of the returned data.
   */
  private Metadata generateMetadata(String description) {
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      long duration = System.currentTimeMillis() - startTime;
      metadata = new Metadata(duration, description,
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    return metadata;
  }

  /**
   * Writes a response in the csv format for /count|length|perimeter|area(/density)(/ratio)|groupBy
   * requests.
   */
  private Consumer<CSVWriter> writeCsvResponse(Object[] resultSet) {
    return writer -> writeCsvResponse(writer, resultSet);
  }

  /** Writing of the CSV response for different types of result sets. */
  private void writeCsvResponse(CSVWriter writer, Object[] resultSet) {
    if (resultSet instanceof ElementsResult[]) {
      ElementsResult[] rs = (ElementsResult[]) resultSet;
      writer.writeNext(new String[] {"timestamp", "value"}, false);
      for (ElementsResult elementsResult : rs) {
        writer.writeNext(new String[] {elementsResult.getTimestamp(),
            String.valueOf(elementsResult.getValue())});
      }
    } else if (resultSet instanceof ContributionsResult[]) {
      ContributionsResult[] rs = (ContributionsResult[]) resultSet;
      writer.writeNext(new String[] {"fromTimestamp", "toTimestamp", "value"}, false);
      for (ContributionsResult contributionsResult : rs) {
        writer.writeNext(new String[] {
            contributionsResult.getFromTimestamp(),
            contributionsResult.getToTimestamp(),
            String.valueOf(contributionsResult.getValue())
        });
      }
    } else if (resultSet instanceof RatioResult[]) {
      RatioResult[] rs = (RatioResult[]) resultSet;
      writer.writeNext(new String[] {"timestamp", "value", "value2", "ratio"}, false);
      for (RatioResult ratioResult : rs) {
        writer.writeNext(
            new String[] {ratioResult.getTimestamp(), String.valueOf(ratioResult.getValue()),
                String.valueOf(ratioResult.getValue2()), String.valueOf(ratioResult.getRatio())});
      }
    } else if (resultSet instanceof GroupByResult[]) {
      GroupByObject[] rs = (GroupByResult[]) resultSet;
      if (resultSet.length == 0) {
        writer.writeNext(new String[] {"timestamp"}, false);
      } else {
        var rows = createCsvResponseForElementsGroupBy(rs);
        writer.writeNext(rows.getLeft().toArray(new String[rows.getLeft().size()]), false);
        writer.writeAll(rows.getRight(), false);
      }
    }
  }

  /** Defines character encoding, content type and cache header in given servlet response object. */
  private void setCsvSettingsInServletResponse() {
    servletResponse.setCharacterEncoding("UTF-8");
    servletResponse.setContentType("text/csv");
    if (!RequestUtils.cacheNotAllowed(processingData.getRequestUrl(),
        processingData.getRequestParameters().getTime())) {
      servletResponse.setHeader("Cache-Control", "no-transform, public, max-age=31556926");
    }
  }

  /**
   * Writes the CSV response directly and returns a null Response as writer has already been called.
   *
   * @throws IOException thrown by {@link javax.servlet.ServletResponse#getWriter() getWriter}
   */
  private Response writeCsv(List<String[]> comments, Consumer<CSVWriter> consumer)
      throws IOException {
    setCsvSettingsInServletResponse();
    try (CSVWriter writer =
        new CSVWriter(servletResponse.getWriter(), ';', CSVWriter.DEFAULT_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);) {
      writer.writeAll(comments, false);
      consumer.accept(writer);
    }
    // no response needed as writer has already been called
    return null;
  }

  /** Creates the comments of the csv response (Attribution, API-Version and optional Metadata). */
  private List<String[]> createCsvTopComments(Metadata metadata) {
    List<String[]> comments = new LinkedList<>();
    comments.add(new String[] {"# Copyright URL: " + URL});
    comments.add(new String[] {"# Copyright Text: " + TEXT});
    comments.add(new String[] {"# API Version: " + Application.API_VERSION});
    if (metadata != null) {
      comments.add(new String[] {"# Execution Time: " + metadata.getExecutionTime()});
      comments.add(new String[] {"# Description: " + metadata.getDescription()});
      if (metadata.getRequestUrl() != null) {
        comments.add(new String[] {"# Request URL: " + metadata.getRequestUrl()});
      }
    }
    return comments;
  }

  /**
   * Creates the csv response for /elements/_/groupBy requests.
   *
   * @param resultSet <code>GroupByObject</code> array containing <code>GroupByResult</code> objects
   *        containing <code>ElementsResult</code> objects
   * @return <code>Pair</code> containing the column names (left) and the data rows (right)
   */
  private ImmutablePair<List<String>, List<String[]>> createCsvResponseForElementsGroupBy(
      GroupByObject[] resultSet) {
    List<String> columnNames = new LinkedList<>();
    columnNames.add("timestamp");
    List<String[]> rows = new LinkedList<>();
    for (int i = 0; i < resultSet.length; i++) {
      GroupByResult groupByResult = (GroupByResult) resultSet[i];
      Object groupByObject = groupByResult.getGroupByObject();
      if (groupByObject instanceof Object[]) {
        Object[] groupByObjectArr = (Object[]) groupByObject;
        columnNames.add(groupByObjectArr[0].toString() + "_" + groupByObjectArr[1].toString());
      } else {
        columnNames.add(groupByObject.toString());
      }
      for (int j = 0; j < groupByResult.getResult().length; j++) {
        ElementsResult elemResult = (ElementsResult) groupByResult.getResult()[j];
        if (i == 0) {
          String[] row = new String[resultSet.length + 1];
          row[0] = elemResult.getTimestamp();
          row[1] = String.valueOf(elemResult.getValue());
          rows.add(row);
        } else {
          rows.get(j)[i + 1] = String.valueOf(elemResult.getValue());
        }
      }
    }
    return new ImmutablePair<>(columnNames, rows);
  }

  /** Fills the ElementsResult array with respective ElementsResult objects. */
  private ElementsResult[] fillElementsResult(SortedMap<OSHDBTimestamp, ? extends Number> entryVal,
      boolean isDensity, DecimalFormat df, Geometry geom) {
    ElementsResult[] results = new ElementsResult[entryVal.entrySet().size()];
    int count = 0;
    for (Entry<OSHDBTimestamp, ? extends Number> entry : entryVal.entrySet()) {
      if (isDensity) {
        results[count] = new ElementsResult(
            TimestampFormatter.getInstance().isoDateTime(entry.getKey()), Double.parseDouble(
                df.format(entry.getValue().doubleValue() / (Geo.areaOf(geom) * 0.000001))));
      } else {
        results[count] =
            new ElementsResult(TimestampFormatter.getInstance().isoDateTime(entry.getKey()),
                Double.parseDouble(df.format(entry.getValue().doubleValue())));
      }
      count++;
    }
    return results;
  }

  /**
   * Computes the result for the /count|length|perimeter|area/groupBy/boundary resources.
   *
   * @throws BadRequestException if a boundary parameter is not defined.
   * @throws Exception thrown by {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator
   *         #count() count}, or
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator
   *         #sum(SerializableFunction) sum}
   */
  private <P extends Geometry & Polygonal> SortedMap<OSHDBCombinedIndex<OSHDBTimestamp, Integer>,
        ? extends Number> computeCountLengthPerimeterAreaGbB(RequestResource requestResource,
        BoundaryType boundaryType, MapReducer<OSMEntitySnapshot> mapRed,
        InputProcessor inputProcessor) throws Exception {
    if (boundaryType == BoundaryType.NOBOUNDARY) {
      throw new BadRequestException(ExceptionMessages.NO_BOUNDARY);
    }
    MapAggregator<OSHDBCombinedIndex<OSHDBTimestamp, Integer>, Geometry> preResult;
    ArrayList<Geometry> arrGeoms = new ArrayList<>(processingData.getBoundaryList());
    @SuppressWarnings("unchecked") // intentionally as check for P on Polygonal is already performed
    Map<Integer, P> geoms = IntStream.range(0, arrGeoms.size()).boxed()
        .collect(Collectors.toMap(idx -> idx, idx -> (P) arrGeoms.get(idx)));
    MapAggregator<OSHDBCombinedIndex<OSHDBTimestamp, Integer>, OSMEntitySnapshot> mapAgg =
        mapRed.aggregateByTimestamp().aggregateByGeometry(geoms);
    if (processingData.isContainingSimpleFeatureTypes()) {
      mapAgg = inputProcessor.filterOnSimpleFeatures(mapAgg);
    }
    Optional<FilterExpression> filter = processingData.getFilterExpression();
    if (filter.isPresent()) {
      mapAgg = mapAgg.filter(filter.get());
    }
    preResult = mapAgg.map(OSMEntitySnapshot::getGeometry);
    switch (requestResource) {
      case COUNT:
        return preResult.count();
      case PERIMETER:
        return preResult.sum(geom -> {
          if (!(geom instanceof Polygonal)) {
            return 0.0;
          }
          return ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom.getBoundary()));
        });
      case LENGTH:
        return preResult
            .sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.lengthOf(geom)));
      case AREA:
        return preResult.sum(geom -> ExecutionUtils.cacheInUserData(geom, () -> Geo.areaOf(geom)));
      default:
        return null;
    }
  }
}
