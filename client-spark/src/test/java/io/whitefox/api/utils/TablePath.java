package io.whitefox.api.utils;

import java.net.URL;

public class TablePath {

  public static String getDeltaTablePath(URL resource) {
    return String.format("%s#%s.%s.%s", resource, "s3share", "s3schemadelta", "s3Table1");
  }
}
