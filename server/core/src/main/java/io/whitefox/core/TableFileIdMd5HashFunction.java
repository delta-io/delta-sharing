package io.whitefox.core;

import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

@ApplicationScoped
public class TableFileIdMd5HashFunction implements TableFileIdHashFunction {

  @Override
  public String hash(String tableFileId) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("md5");
      md5.update(tableFileId.getBytes(StandardCharsets.UTF_8));
      byte[] digest = md5.digest();
      return DatatypeConverter.printHexBinary(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
