package io.whitefox.api.deltasharing.encoders;

import io.whitefox.core.services.ContentAndToken;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaPageTokenEncoderTest {

  DeltaPageTokenEncoder encoder = new DeltaPageTokenEncoder();

  @Test
  public void testTokenEncoding() {
    ContentAndToken.Token id = new ContentAndToken.Token(123);
    String encoded = encoder.encodePageToken(id);
    ContentAndToken.Token decoded = encoder.decodePageToken(encoded);
    Assertions.assertEquals(id, decoded);
  }
}
