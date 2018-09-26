package org.heigit.bigspatialdata.ohsome.ohsomeapi.controller;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertTrue;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.Application;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import com.fasterxml.jackson.databind.JsonNode;

/** Test class for all of the controller classes sending POST requests. */
public class PostControllerTest {

  private static String port = TestProperties.PORT2;
  private String server = TestProperties.SERVER;

  /** Method to start this application context. */
  @BeforeClass
  public static void applicationMainStartup() {
    assumeTrue(
        TestProperties.INTEGRATION == null || !TestProperties.INTEGRATION.equalsIgnoreCase("no"));
    // this instance gets reused by all of the following @Test methods
    Application.main(new String[] {TestProperties.DB_FILE_PATH_PROPERTY, "--port=" + port});
  }

  /*
   * /elements/count tests
   */

  @Test
  public void elementsCountTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.67452,49.40961,8.70392,49.41823");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/count", map, JsonNode.class);
    assertTrue(response.getBody().get("result").get(0).get("value").asInt() == 40);
  }

  @Test
  public void elementsCountGroupByBoundaryGeoJsonTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bpolys",
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{},"
            + "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[8.68494,49.41951],"
            + "[8.67902,49.41460],[8.69009,49.41527],[8.68494,49.41951]]]}},{\"type\":\"Feature\","
            + "\"properties\":{},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[8.68812,"
            + "49.40466],[8.68091,49.40058],[8.69121,49.40069],[8.68812,49.40466]]]}}]}");
    map.add("types", "way");
    map.add("time", "2016-01-01");
    map.add("keys", "building");
    map.add("format", "geojson");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/count/groupBy/boundary", map, JsonNode.class);
    assertTrue(
        response.getBody().get("features").get(0).get("properties").get("value").asInt() == 367);
  }

  /*
   * /elements/perimeter tests
   */

  @Test
  public void elementsPerimeterTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/perimeter", map, JsonNode.class);
    assertTrue(response.getBody().get("result").get(0).get("value").asDouble() == 571.84);
  }

  @Test
  public void elementsPerimeterGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,"
        + "49.41256,8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(0).get("result").get(0).get("value")
        .asDouble() == 2476.29);
  }

  @Test
  public void elementsPerimeterGroupByTypeTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way,relation");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/groupBy/type", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 999.13);
  }

  @Test
  public void elementsPerimeterGroupByKeyTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("groupByKeys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/groupBy/key", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 64127.88);
  }

  @Test
  public void elementsPerimeterGroupByTagTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("groupByKey", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/groupBy/tag", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 59012.08);
  }

  @Test
  public void elementsPerimeterGroupByUserTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/groupBy/user", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 4.86);
  }

  @Test
  public void elementsPerimeterShareTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/share", map, JsonNode.class);
    assertTrue(response.getBody().get("shareResult").get(0).get("part").asDouble() == 64127.88);
  }

  @Test
  public void elementsPerimeterShareGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,"
        + "49.41256,8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/perimeter/share/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("shareGroupByBoundaryResult").get(1).get("shareResult").get(0)
        .get("part").asDouble() == 108415.17);
  }

  @Test
  public void elementsPerimeterRatioTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("keys", "building");
    map.add("time", "2015-01-01");
    map.add("types2", "relation");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/ratio", map, JsonNode.class);
    assertTrue(response.getBody().get("ratioResult").get(0).get("ratio").asDouble() == 0.01558);
  }

  @Test
  public void elementsPerimeterRatioGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,49.41256,"
        + "8.69304,49.42331");
    map.add("types", "way");
    map.add("keys", "building");
    map.add("time", "2015-01-01");
    map.add("values", "yes");
    map.add("types2", "relation");
    map.add("keys2", "building");
    map.add("values2", "yes");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/perimeter/ratio/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByBoundaryResult").get(1).get("ratioResult").get(0)
        .get("ratio").asDouble() == 0.008612);
  }

  @Test
  public void elementsPerimeterDensityTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/perimeter/density", map, JsonNode.class);
    assertTrue(response.getBody().get("result").get(0).get("value").asDouble() == 2130.19);
  }

  @Test
  public void elementsPerimeterDensityGroupByTypeTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way,relation");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/perimeter/density/groupBy/type", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 990.96);
  }

  @Test
  public void elementsPerimeterDensityGroupByTagTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("groupByKey", "building");
    map.add("groupByValues", "yes");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/perimeter/density/groupBy/tag", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(0).get("result").get(0).get("value")
        .asDouble() == 5073.94);
  }

  @Test
  public void elementsPerimeterDensityGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,49.41256,"
        + "8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/perimeter/density/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 455);
  }

  /*
   * /elements/area tests
   */

  @Test
  public void elementsAreaTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/area", map, JsonNode.class);
    assertTrue(response.getBody().get("result").get(0).get("value").asDouble() == 1845.85);
  }

  @Test
  public void elementsAreaGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,49.41256,"
        + "8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2015-01-01");
    map.add("keys", "building");
    map.add("values", "residential");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 1861.71);
  }

  @Test
  public void elementsAreaGroupByTypeTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way,relation");
    map.add("time", "2017-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/groupBy/type", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 22216.19);
  }

  @Test
  public void elementsAreaGroupByKeyTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("groupByKeys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/groupBy/key", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 263901.91);
  }

  @Test
  public void elementsAreaGroupByTagTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("groupByKey", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/groupBy/tag", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 244077.2);
  }

  @Test
  public void elementsAreaGroupByUserTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/groupBy/user", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(0).get("result").get(0).get("value")
        .asDouble() == 168);
  }

  @Test
  public void elementsAreaShareTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/area/share", map, JsonNode.class);
    assertTrue(response.getBody().get("shareResult").get(0).get("part").asDouble() == 263901.91);
  }

  @Test
  public void elementsAreaShareGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,"
        + "49.41256,8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/area/share/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("shareGroupByBoundaryResult").get(1).get("shareResult").get(0)
        .get("part").asDouble() == 356090.4);
  }

  @Test
  public void elementsAreaRatioTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.68081,49.39821,8.69528,49.40687");
    map.add("types", "way");
    map.add("keys", "building");
    map.add("time", "2017-01-01");
    map.add("types2", "relation");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/area/ratio", map, JsonNode.class);
    assertTrue(response.getBody().get("ratioResult").get(0).get("ratio").asDouble() == 0.084184);
  }

  @Test
  public void elementsAreaRatioGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,49.41256,"
        + "8.69304,49.42331");
    map.add("types", "way");
    map.add("keys", "building");
    map.add("time", "2017-01-01");
    map.add("types2", "relation");
    map.add("keys2", "building");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/area/ratio/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByBoundaryResult").get(1).get("ratioResult").get(0)
        .get("ratio").asDouble() == 0.023699);
  }

  @Test
  public void elementsAreaDensityTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("keys", "building");
    map.add("values", "yes");
    ResponseEntity<JsonNode> response =
        restTemplate.postForEntity(server + port + "/elements/area/density", map, JsonNode.class);
    assertTrue(response.getBody().get("result").get(0).get("value").asDouble() == 404281.84);
  }

  @Test
  public void elementsAreaDensityGroupByTypeTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way,relation");
    map.add("time", "2017-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/density/groupBy/type", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 26198.91);
  }

  @Test
  public void elementsAreaDensityGroupByTagTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "8.69416,49.40969,8.71154,49.41161");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("groupByKey", "building");
    ResponseEntity<JsonNode> response = restTemplate
        .postForEntity(server + port + "/elements/area/density/groupBy/tag", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(1).get("result").get(0).get("value")
        .asDouble() == 404281.84);
  }

  @Test
  public void elementsAreaDensityGroupByBoundaryTest() {
    TestRestTemplate restTemplate = new TestRestTemplate();
    MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
    map.add("bboxes", "Weststadt:8.68081,49.39821,8.69528,49.40687|Neuenheim:8.67691,49.41256,"
        + "8.69304,49.42331");
    map.add("types", "way");
    map.add("time", "2017-01-01");
    map.add("keys", "building");
    ResponseEntity<JsonNode> response = restTemplate.postForEntity(
        server + port + "/elements/area/density/groupBy/boundary", map, JsonNode.class);
    assertTrue(response.getBody().get("groupByResult").get(0).get("result").get(0).get("value")
        .asDouble() == 261743.13);
  }
}