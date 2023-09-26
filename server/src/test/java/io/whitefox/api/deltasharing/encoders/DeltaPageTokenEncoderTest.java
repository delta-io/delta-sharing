package io.whitefox.api.deltasharing.encoders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaPageTokenEncoderTest {

  DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();

  @Test
  public void testTokenEncoding() {
    String id = "SomeId";
    String encoded = encoder.encodePageToken(id);
    String decoded = encoder.decodePageToken(encoded);
    Assertions.assertEquals(id, decoded);
  }
}
