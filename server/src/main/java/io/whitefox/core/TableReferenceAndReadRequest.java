package io.whitefox.core;

public class TableReferenceAndReadRequest {
  private final String share;
  private final String schema;
  private final String table;
  private final ReadTableRequest readTableRequest;

  public TableReferenceAndReadRequest(
      String share, String schema, String table, ReadTableRequest readTableRequest) {
    this.share = share;
    this.schema = schema;
    this.table = table;
    this.readTableRequest = readTableRequest;
  }

  public String share() {
    return share;
  }

  public String schema() {
    return schema;
  }

  public String table() {
    return table;
  }

  public ReadTableRequest readTableRequest() {
    return readTableRequest;
  }
}
