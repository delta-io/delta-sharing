package io.whitefox.api.deltasharing.encoders;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.whitefox.core.services.ContentAndToken;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Base64;

@ApplicationScoped
public class DeltaPageTokenEncoder {

  public String encodePageToken(ContentAndToken.Token id) {
    return new String(
        Base64.getUrlEncoder().encode(Integer.toString(id.value()).getBytes(UTF_8)), UTF_8);
  }

  public ContentAndToken.Token decodePageToken(String encodedPageToken) {
    return new ContentAndToken.Token(
        Integer.parseInt(new String(Base64.getMimeDecoder().decode(encodedPageToken), UTF_8)));
  }
}
