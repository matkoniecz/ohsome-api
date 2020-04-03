package org.heigit.bigspatialdata.ohsome.ohsomeapi.executor;

import javax.servlet.http.HttpServletRequest;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.Application;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.exception.BadRequestException;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.oshdb.ExtractMetadata;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.output.dataaggregationresponse.Attribution;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.output.metadataresponse.ExtractRegion;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.output.metadataresponse.MetadataResponse;
import org.heigit.bigspatialdata.ohsome.ohsomeapi.output.metadataresponse.TemporalExtent;

/** Includes the execute method for requests mapped to /metadata. */
public class MetadataRequestExecutor {

  /**
   * Returns the metadata of the underlying extract-file.
   * 
   * @return {@link org.heigit.bigspatialdata.ohsome.ohsomeapi.output.metadataresponse.MetadataResponse
   *         MetadataResponse}
   */
  public static MetadataResponse executeGetMetadata(HttpServletRequest servletRequest) {
    if (!servletRequest.getParameterMap().isEmpty()) {
      throw new BadRequestException("The endpoint 'metadata' does not require parameters");
    }
    return new MetadataResponse(
        new Attribution(ExtractMetadata.attributionUrl, ExtractMetadata.attributionShort),
        Application.API_VERSION,
        new ExtractRegion(ExtractMetadata.dataPolyJson,
            new TemporalExtent(ExtractMetadata.fromTstamp, ExtractMetadata.toTstamp),
            ExtractMetadata.replicationSequenceNumber));
  }

  private MetadataRequestExecutor() {
    throw new IllegalStateException("Utility class");
  }
}
