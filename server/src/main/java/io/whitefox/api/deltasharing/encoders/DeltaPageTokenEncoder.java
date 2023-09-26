package io.whitefox.api.deltasharing.encoders;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Base64;

@ApplicationScoped
public class DeltaPageTokenEncoder {

  public String encodePageToken(String id) {
    return new String(Base64.getUrlEncoder().encode(id.getBytes(UTF_8)), UTF_8);
  }

  public String decodePageToken(String encodedPageToken) {
    return new String(Base64.getMimeDecoder().decode(encodedPageToken), UTF_8);
  }
}
