package org.heigit.ohsome.ohsomeapi.executor.dataextraction;

import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite.ComputeMode;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser;
import org.heigit.bigspatialdata.oshdb.util.time.TimestampFormatter;
import org.heigit.ohsome.filter.FilterExpression;
import org.heigit.ohsome.ohsomeapi.Application;
import org.heigit.ohsome.ohsomeapi.controller.dataextraction.ElementsGeometry;
import org.heigit.ohsome.ohsomeapi.exception.BadRequestException;
import org.heigit.ohsome.ohsomeapi.exception.ExceptionMessages;
import org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils;
import org.heigit.ohsome.ohsomeapi.executor.RequestExecutor;
import org.heigit.ohsome.ohsomeapi.executor.RequestParameters;
import org.heigit.ohsome.ohsomeapi.executor.RequestResource;
import org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessingUtils;
import org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor;
import org.heigit.ohsome.ohsomeapi.inputprocessing.ProcessingData;
import org.heigit.ohsome.ohsomeapi.inputprocessing.SimpleFeatureType;
import org.heigit.ohsome.ohsomeapi.oshdb.DbConnData;
import org.heigit.ohsome.ohsomeapi.output.Attribution;
import org.heigit.ohsome.ohsomeapi.output.ExtractionResponse;
import org.heigit.ohsome.ohsomeapi.output.Metadata;
import org.locationtech.jts.geom.Geometry;
import org.wololo.geojson.Feature;

/** Holds executor methods for the following endpoints: /elementsFullHistory, /contributions. */
public class ExtractionExecutor extends RequestExecutor {

  private final RequestResource requestResource;
  private final InputProcessor inputProcessor;
  private final ProcessingData processingData;
  private final ElementsGeometry elementsGeometry;

  public ExtractionExecutor(RequestResource requestResource, ElementsGeometry elementsGeometry,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
    super(servletRequest, servletResponse);
    this.requestResource = requestResource;
    this.elementsGeometry = elementsGeometry;
    inputProcessor = new InputProcessor(servletRequest, false, false);
    processingData = inputProcessor.getProcessingData();
  }

  /**
   * Performs an OSM data extraction using the full-history of the data.
   *
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters},
   *         {@link org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser
   *         #parseIsoDateTime(String) parseIsoDateTime},
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer#stream() stream}, or
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #streamResponse(HttpServletResponse, ExtractionResponse, Stream)
   *         streamElementsResponse}
   */
  public void extract() throws Exception {
    inputProcessor.getProcessingData().setFullHistory(true);
    InputProcessor snapshotInputProcessor = new InputProcessor(servletRequest, true, false);
    snapshotInputProcessor.getProcessingData().setFullHistory(true);
    MapReducer<OSMEntitySnapshot> mapRedSnapshot = null;
    MapReducer<OSMContribution> mapRedContribution = null;
    if (DbConnData.db instanceof OSHDBIgnite) {
      // on ignite: Use AffinityCall backend, which is the only one properly supporting streaming
      // of result data, without buffering the whole result in memory before returning the result.
      // This allows to write data out to the client via a chunked HTTP response.
      mapRedSnapshot = snapshotInputProcessor.processParameters(ComputeMode.AffinityCall);
      mapRedContribution = inputProcessor.processParameters(ComputeMode.AffinityCall);
    } else {
      mapRedSnapshot = snapshotInputProcessor.processParameters();
      mapRedContribution = inputProcessor.processParameters();
    }
    RequestParameters requestParameters = processingData.getRequestParameters();
    String[] time = inputProcessor.splitParamOnComma(
        inputProcessor.createEmptyArrayIfNull(servletRequest.getParameterValues("time")));
    if (time.length != 2) {
      throw new BadRequestException(ExceptionMessages.TIME_FORMAT_FULL_HISTORY);
    }
    TagTranslator tt = DbConnData.tagTranslator;
    String[] keys = requestParameters.getKeys();
    final Set<Integer> keysInt = ExecutionUtils.keysToKeysInt(keys, tt);
    final ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    inputProcessor.processPropertiesParam();
    inputProcessor.processIsUnclippedParam();
    InputProcessingUtils utils = inputProcessor.getUtils();
    final boolean includeTags = inputProcessor.includeTags();
    final boolean includeOSMMetadata = inputProcessor.includeOSMMetadata();
    final boolean includeContributionTypes = inputProcessor.includeContributionTypes();
    final boolean clipGeometries = inputProcessor.isClipGeometry();
    final boolean isContributionsLatestEndpoint =
        requestResource.equals(RequestResource.CONTRIBUTIONSLATEST);
    final boolean isContributionsEndpoint =
        isContributionsLatestEndpoint || requestResource.equals(RequestResource.CONTRIBUTIONS);
    final Set<SimpleFeatureType> simpleFeatureTypes = processingData.getSimpleFeatureTypes();
    String startTimestamp = IsoDateTimeParser.parseIsoDateTime(requestParameters.getTime()[0])
        .format(DateTimeFormatter.ISO_DATE_TIME);
    String endTimestamp = IsoDateTimeParser.parseIsoDateTime(requestParameters.getTime()[1])
        .format(DateTimeFormatter.ISO_DATE_TIME);
    MapReducer<List<OSMContribution>> mapRedContributions = mapRedContribution.groupByEntity();
    MapReducer<List<OSMEntitySnapshot>> mapRedSnapshots = mapRedSnapshot.groupByEntity();
    Optional<FilterExpression> filter = processingData.getFilterExpression();
    if (filter.isPresent()) {
      mapRedSnapshots = mapRedSnapshots.filter(filter.get());
      mapRedContributions = mapRedContributions.filter(filter.get());
    }
    final boolean isContainingSimpleFeatureTypes = processingData.isContainingSimpleFeatureTypes();
    ExtractionTransformer dataExtractionTransformer = new ExtractionTransformer(
        startTimestamp, endTimestamp, filter.orElse(null), isContributionsEndpoint,
        isContributionsLatestEndpoint,
        clipGeometries, includeTags, includeOSMMetadata, includeContributionTypes, utils, exeUtils,
        keysInt, elementsGeometry, simpleFeatureTypes,
        isContainingSimpleFeatureTypes);
    MapReducer<Feature> contributionPreResult = mapRedContributions
        .flatMap(dataExtractionTransformer::buildChangedFeatures)
        .filter(Objects::nonNull);
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      metadata = new Metadata(null, requestResource.getDescription(),
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    ExtractionResponse osmData = new ExtractionResponse(ATTRIBUTION, Application.API_VERSION,
        metadata, "FeatureCollection", Collections.emptyList());
    MapReducer<Feature> snapshotPreResult = null;
    if (!isContributionsEndpoint) {
      // handles cases where valid_from = t_start, valid_to = t_end; i.e. non-modified data
      snapshotPreResult = mapRedSnapshots
          .filter(snapshots -> snapshots.size() == 2)
          .filter(snapshots -> snapshots.get(0).getGeometry() == snapshots.get(1).getGeometry()
              && snapshots.get(0).getEntity().getVersion() == snapshots.get(1).getEntity()
                  .getVersion())
          .map(snapshots -> snapshots.get(0))
          .flatMap(dataExtractionTransformer::buildUnchangedFeatures)
          .filter(Objects::nonNull);
    }
    try (
        Stream<Feature> snapshotStream =
            (snapshotPreResult != null) ? snapshotPreResult.stream() : Stream.empty();
        Stream<Feature> contributionStream = contributionPreResult.stream()) {
      exeUtils.streamResponse(servletResponse, osmData,
          Stream.concat(contributionStream, snapshotStream));
    }
  }
  
  /**
   * Performs an OSM data extraction.
   *
   * @param elemGeom {@link
   *        org.heigit.ohsome.ohsomeapi.controller.dataextraction.ElementsGeometry
   *        ElementsGeometry} defining the geometry of the OSM elements
   * @param servletRequest {@link javax.servlet.http.HttpServletRequest HttpServletRequest} incoming
   *        request object
   * @param servletResponse {@link javax.servlet.http.HttpServletResponse HttpServletResponse]}
   *        outgoing response object
   * @throws Exception thrown by {@link org.heigit.ohsome.ohsomeapi.inputprocessing.InputProcessor
   *         #processParameters() processParameters},
   *         {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer#stream() stream}, or
   *         {@link org.heigit.ohsome.ohsomeapi.executor.ExecutionUtils
   *         #streamResponse(HttpServletResponse, ExtractionResponse, Stream)
   *         streamElementsResponse}
   */
  public static void extract(RequestResource requestResource, ElementsGeometry elemGeom,
      HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws Exception {
    InputProcessor inputProcessor = new InputProcessor(servletRequest, true, false);
    MapReducer<OSMEntitySnapshot> mapRed = null;
    inputProcessor.processPropertiesParam();
    inputProcessor.processIsUnclippedParam();
    final boolean includeTags = inputProcessor.includeTags();
    final boolean includeOSMMetadata = inputProcessor.includeOSMMetadata();
    final boolean clipGeometries = inputProcessor.isClipGeometry();
    if (DbConnData.db instanceof OSHDBIgnite) {
      // on ignite: Use AffinityCall backend, which is the only one properly supporting streaming
      // of result data, without buffering the whole result in memory before returning the result.
      // This allows to write data out to the client via a chunked HTTP response.
      mapRed = inputProcessor.processParameters(ComputeMode.AffinityCall);
    } else {
      mapRed = inputProcessor.processParameters();
    }
    ProcessingData processingData = inputProcessor.getProcessingData();
    RequestParameters requestParameters = processingData.getRequestParameters();
    TagTranslator tt = DbConnData.tagTranslator;
    String[] keys = requestParameters.getKeys();
    final Set<Integer> keysInt = ExecutionUtils.keysToKeysInt(keys, tt);
    final MapReducer<Feature> preResult;
    final ExecutionUtils exeUtils = new ExecutionUtils(processingData);
    preResult = mapRed.map(snapshot -> {
      Map<String, Object> properties = new TreeMap<>();
      if (includeOSMMetadata) {
        properties.put("@lastEdit",
            TimestampFormatter.getInstance().isoDateTime(snapshot.getEntity().getTimestamp()));
      }
      properties.put("@snapshotTimestamp",
          TimestampFormatter.getInstance().isoDateTime(snapshot.getTimestamp()));
      Geometry geom = snapshot.getGeometry();
      if (!clipGeometries) {
        geom = snapshot.getGeometryUnclipped();
      }
      return exeUtils.createOSMFeature(snapshot.getEntity(), geom, properties, keysInt, includeTags,
          includeOSMMetadata, false, false, elemGeom,
          EnumSet.noneOf(ContributionType.class));
    }).filter(Objects::nonNull);
    Metadata metadata = null;
    if (processingData.isShowMetadata()) {
      metadata = new Metadata(null, requestResource.getDescription(),
          inputProcessor.getRequestUrlIfGetRequest(servletRequest));
    }
    ExtractionResponse osmData = new ExtractionResponse(new Attribution(URL, TEXT),
        Application.API_VERSION, metadata, "FeatureCollection", Collections.emptyList());
    try (Stream<Feature> streamResult = preResult.stream()) {
      exeUtils.streamResponse(servletResponse, osmData, streamResult);
    }
  }
}
