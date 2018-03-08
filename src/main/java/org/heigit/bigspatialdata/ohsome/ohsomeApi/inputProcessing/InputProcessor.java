package org.heigit.bigspatialdata.ohsome.ohsomeApi.inputProcessing;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.Application;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.executor.RequestResource;
import org.heigit.bigspatialdata.ohsome.ohsomeApi.exception.BadRequestException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;

/**
 * Holds general input processing and validation methods and validates specific parameters given by
 * the request. Uses geometry methods from
 * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.inputProcessing.GeometryBuilder
 * GeometryBuilder} and utils from
 * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.inputProcessing.Utils Utils}. Throws exceptions
 * depending on their validity.
 */
public class InputProcessor {

  private BoundaryType boundary;
  private String[] boundaryValues;
  private EnumSet<OSMType> osmTypes;
  private String[] timeData;
  private boolean showMetadata;
  private GeometryBuilder geomBuilder;
  private Utils utils;

  /**
   * Processes the input parameters from the given request.
   * <p>
   * The other parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.elements.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @param isPost <code>Boolean</code> value defining if it is a POST (true) or GET (false)
   *        request.
   * @return {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer MapReducer} object
   *         including the settings derived from the given parameters.
   */
  public MapReducer<OSMEntitySnapshot> processParameters(boolean isPost, String bboxes,
      String bcircles, String bpolys, String[] types, String[] keys, String[] values,
      String[] userids, String[] time, String showMetadata) throws Exception {

    // set earliest/latest timestamps (if in metadata)
    if (Application.getMetadata() != null && !Application.getMetadata().get(0).isEmpty()
        && !Application.getMetadata().get(2).isEmpty()) {
      Utils.defStartTime = Application.getMetadata().get(0);
      Utils.defEndTime = Application.getMetadata().get(1);
    } else {
      Utils.defStartTime = "2007-11-01T00:00:00";
      Utils.defEndTime = "2018-03-01T00:00:00";
    }
    geomBuilder = new GeometryBuilder();
    utils = new Utils();

    if (isPost) {
      bboxes = createEmptyStringIfNull(bboxes);
      bcircles = createEmptyStringIfNull(bcircles);
      bpolys = createEmptyStringIfNull(bpolys);
      types = createEmptyArrayIfNull(types);
      keys = createEmptyArrayIfNull(keys);
      values = createEmptyArrayIfNull(values);
      userids = createEmptyArrayIfNull(userids);
      time = createEmptyArrayIfNull(time);
    }
    MapReducer<OSMEntitySnapshot> mapRed = null;

    // database
    if (Application.getKeytables() == null)
      mapRed = OSMEntitySnapshotView.on(Application.getH2Db());
    else if (Application.getIgniteDb() == null)
      mapRed =
          OSMEntitySnapshotView.on(Application.getH2Db()).keytables(Application.getKeytables());
    else
      mapRed =
          OSMEntitySnapshotView.on(Application.getIgniteDb()).keytables(Application.getKeytables());

    // metadata
    if (showMetadata == null)
      this.showMetadata = false;
    else if (showMetadata.equals("true"))
      this.showMetadata = true;
    else if (showMetadata.equals("false") || showMetadata.equals(""))
      this.showMetadata = false;
    else
      throw new BadRequestException(
          "The showMetadata parameter can only contain the values 'true' or 'false' written as text(String).");

    checkBoundaryParams(bboxes, bcircles, bpolys);

    switch (boundary) {
      case NOBOUNDARY:
        mapRed = mapRed.areaOfInterest(geomBuilder.createBbox(new String[0]));
        break;
      case BBOXES:
        boundaryValues = utils.splitBoundaryParam(bboxes, (byte) 1);
        mapRed =
            mapRed.areaOfInterest((Geometry & Polygonal) geomBuilder.createBboxes(boundaryValues));
        break;
      case BCIRCLES:
        boundaryValues = utils.splitBoundaryParam(bcircles, (byte) 2);
        mapRed = mapRed.areaOfInterest(
            (Geometry & Polygonal) geomBuilder.createCircularPolygons(boundaryValues));
        break;
      case BPOLYS:
        boundaryValues = utils.splitBoundaryParam(bpolys, (byte) 3);
        mapRed =
            mapRed.areaOfInterest((Geometry & Polygonal) geomBuilder.createBpolys(boundaryValues));
        break;
      default:
        throw new BadRequestException(
            "Your provided boundary parameter (bboxes, bcircles, or bpolys) does not fit its format. "
                + "or you defined more than one boundary parameter.");
    }

    mapRed = mapRed.osmTypes(checkTypes(types));

    if (time.length == 1) {
      timeData = utils.extractIsoTime(time[0]);
      if (timeData[2] != null) {
        // interval is given
        mapRed = mapRed.timestamps(new OSHDBTimestamps(timeData[0], timeData[1], timeData[2]));
      } else
        mapRed = mapRed.timestamps(timeData[0], timeData[1]);
    } else if (time.length == 0) {
      // no time parameter --> return default end time
      mapRed = mapRed.timestamps(Utils.defEndTime);
    } else {
      // list of timestamps
      int tCount = 1;
      for (String timestamp : time) {
        utils.checkIsoConformity(timestamp, "timestamp number " + tCount);
        tCount++;
      }
      String firstElem = time[0];
      time = ArrayUtils.remove(time, 0);
      mapRed = mapRed.timestamps(firstElem, firstElem, time);
    }

    mapRed = checkKeysValues(mapRed, keys, values);

    if (userids.length != 0) {
      checkUserids(userids);
      Set<Integer> useridSet = new HashSet<>();
      for (String user : userids) {
        useridSet.add(Integer.valueOf(user));
      }

      mapRed = mapRed.where(entity -> {
        return useridSet.contains(entity.getUserId());
      });
    } else {
      // do nothing --> all users will be used
    }

    return mapRed;
  }

  /**
   * Checks the given unit parameter. If it is null or empty --> unit gets the default value
   * depending on the <code>RequestResource</code>: 'meters' for length|perimeter and
   * 'square-meters' for area.
   * 
   * @param unit
   * @param requestResource
   * @return <code>String</code> defining the given or default unit.
   */
  public String setDefaultUnitIfNullOrEmpty(String unit, RequestResource requestResource) {

    if (unit == null || unit.length() == 0) {
      switch (requestResource) {
        case LENGTH:
        case PERIMETER:
          unit = "meter";
          break;
        case AREA:
          unit = "square-meter";
          break;
      }
    }
    return unit;
  }

  /**
   * Processes the given unit parameter and returns the corresponding factor. Throws an exception if
   * the given unit does not fit to the request resource.
   * 
   * @param unit
   * @param requestResource
   * @return <code>Double</code> value defining the factor.
   */
  public double processUnit(String unit, RequestResource requestResource)
      throws BadRequestException {

    switch (unit) {
      case "meter":
      case "meters":
        if (requestResource == RequestResource.AREA)
          throw new BadRequestException("You cannot define a unit of length for an area request.");
        return 1;
      case "kilometer":
      case "kilometers":
        if (requestResource == RequestResource.AREA)
          throw new BadRequestException("You cannot define a unit of length for an area request.");
        return 0.001;
      case "square-meter":
      case "square-meters":
        if (requestResource == RequestResource.PERIMETER
            || requestResource == RequestResource.LENGTH)
          throw new BadRequestException(
              "You cannot define a unit of area for a length|perimeter request.");
        return 1;
      case "square-kilometer":
      case "square-kilometers":
        if (requestResource == RequestResource.PERIMETER
            || requestResource == RequestResource.LENGTH)
          throw new BadRequestException(
              "You cannot define a unit of area for a length|perimeter request.");
        return 0.000001;
      default:
        throw new BadRequestException(
            "The defined unit parameter is invalid. You can only choose between meter(s), "
                + "or kilometer(s) for length|perimeter and square-meter(s), or square-kilometer(s) for area.");
    }
  }

  /**
   * Checks the given boundary parameter(s) and sets a corresponding enum (NOBOUNDARY for no
   * boundary, BBOXES for bboxes, bcircleS for bcircles, BPOLYS for bpolys). Only one (or none) of
   * them is allowed to have content in it.
   * 
   * @param bboxes <code>String</code> containing the bounding boxes separated via a pipe (|) and
   *        optional custom names at each first coordinate appended with a colon (:).
   * @param bcircles <code>String</code> containing the bounding points separated via a pipe (|) and
   *        optional custom names at each first coordinate appended with a colon (:).
   * @param bpolys <code>String</code> containing the bounding polygons separated via a pipe (|) and
   *        optional custom names at each first coordinate appended with a colon (:).
   */
  private void checkBoundaryParams(String bboxes, String bcircles, String bpolys) {
    if (bboxes.isEmpty() && bcircles.isEmpty() && bpolys.isEmpty()) {
      boundary = BoundaryType.NOBOUNDARY;
    } else if (!bboxes.isEmpty() && bcircles.isEmpty() && bpolys.isEmpty()) {
      boundary = BoundaryType.BBOXES;
    } else if (bboxes.isEmpty() && !bcircles.isEmpty() && bpolys.isEmpty()) {
      boundary = BoundaryType.BCIRCLES;
    } else if (bboxes.isEmpty() && bcircles.isEmpty() && !bpolys.isEmpty()) {
      boundary = BoundaryType.BPOLYS;
    } else
      throw new BadRequestException(
          "Your provided boundary parameter (bboxes, bcircles, or bpolys) does not fit its format, "
              + "or you defined more than one boundary parameter.");
  }

  /**
   * Checks and extracts the content of the types parameter.
   * 
   * @param types <code>String</code> array containing one, two, or all 3 OSM types (node, way,
   *        relation). If the array is empty, all three types are used.
   * @return <code>EnumSet</code> containing the requested OSM type(s).
   * @throws BadRequestException if the content of the parameter does not represent one, two, or all
   *         three OSM types
   */
  private EnumSet<OSMType> checkTypes(String[] types) throws BadRequestException {
    if (types.length > 3) {
      throw new BadRequestException(
          "Parameter 'types' containing the OSM Types cannot have more than 3 entries.");
    } else if (types.length == 0) {
      this.osmTypes = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
      return this.osmTypes;
    } else {
      this.osmTypes = EnumSet.noneOf(OSMType.class);
      for (String type : types) {
        if (type.equals("node"))
          this.osmTypes.add(OSMType.NODE);
        else if (type.equals("way"))
          this.osmTypes.add(OSMType.WAY);
        else if (type.equals("relation"))
          this.osmTypes.add(OSMType.RELATION);
        else
          throw new BadRequestException(
              "Parameter 'types' can only have 'node' and/or 'way' and/or 'relation' as its content.");
      }
      return this.osmTypes;
    }
  }

  /**
   * Checks the given keys and values parameters on their length and includes them in the
   * {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer#where(String) where(key)}, or
   * {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer#where(String, String)
   * where(key, value)} method.
   * <p>
   * The keys and values parameters are described in the
   * {@link org.heigit.bigspatialdata.ohsome.ohsomeApi.controller.elements.CountController#getCount(String, String, String, String[], String[], String[], String[], String[], String)
   * getCount} method.
   * 
   * @param mapRed current {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer
   *        MapReducer} object
   * @return {@link org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer MapReducer} object
   *         including the filters derived from the given parameters.
   * @throws BadRequestException if there are more values than keys given
   */
  private MapReducer<OSMEntitySnapshot> checkKeysValues(MapReducer<OSMEntitySnapshot> mapRed,
      String[] keys, String[] values) throws BadRequestException {
    if (keys.length < values.length) {
      throw new BadRequestException(
          "There cannot be more values than keys. For each value in the values parameter, the respective key has to be provided at the same index in the keys parameter.");
    }
    if (keys.length != values.length) {
      String[] tempVal = new String[keys.length];
      for (int a = 0; a < values.length; a++) {
        tempVal[a] = values[a];
      }
      // adds empty entries in the tempVal array
      for (int i = values.length; i < keys.length; i++) {
        tempVal[i] = "";
      }
      values = tempVal;
    }
    // prerequisites: both arrays (keys and values) must be of the same length
    // and key-value pairs need to be at the same index in both arrays
    for (int i = 0; i < keys.length; i++) {
      if (values[i].equals(""))
        mapRed = mapRed.where(keys[i]);
      else
        mapRed = mapRed.where(keys[i], values[i]);
    }
    return mapRed;
  }

  /**
   * Checks the content of the userids <code>String</code> array.
   * 
   * @param userids String array containing the OSM user IDs.
   * @throws BadRequestException if one of the userids is invalid
   */
  private void checkUserids(String[] userids) {
    for (String user : userids) {
      try {
        Long.valueOf(user);
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "The userids parameter can only contain valid OSM userids, which are always a positive whole number");
      }
    }
  }

  /**
   * Creates an empty array if an input parameter of a POST request is null.
   * 
   * @param toCheck <code>String</code> array, which is checked.
   * @return <code>String</code> array, which is empty.
   */
  private String[] createEmptyArrayIfNull(String[] toCheck) {
    if (toCheck == null)
      toCheck = new String[0];
    return toCheck;
  }

  /**
   * Creates an empty <code>String</code>, if a given boundary input parameter of a POST request is
   * null.
   * 
   * @param toCheck <code>String</code>, which is checked.
   * @return <code>String</code>, which is empty, but not null.
   */
  private String createEmptyStringIfNull(String toCheck) {
    if (toCheck == null)
      toCheck = "";
    return toCheck;
  }

  /*
   * Getters start here
   */
  public BoundaryType getBoundaryType() {
    return boundary;
  }

  public String[] getBoundaryValues() {
    return boundaryValues;
  }

  public boolean getShowMetadata() {
    return this.showMetadata;
  }

  public EnumSet<OSMType> getOsmTypes() {
    return osmTypes;
  }

  public GeometryBuilder getGeomBuilder() {
    return geomBuilder;
  }

  public Utils getUtils() {
    return utils;
  }

}