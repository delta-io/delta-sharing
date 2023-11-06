package io.whitefox.core.services;

import io.whitefox.core.*;
import io.whitefox.core.actions.CreateInternalTable;
import io.whitefox.core.actions.CreateProvider;
import io.whitefox.core.actions.CreateStorage;
import java.util.Optional;

public class ShareServiceFixture {
  public static InternalTable setupInternalTable(
      StorageService storageService,
      ProviderService providerService,
      TableService tableService,
      Principal testPrincipal,
      String storageName,
      String providerName,
      String tableName) {
    var storageObj = storageService.createStorage(new CreateStorage(
        storageName,
        Optional.empty(),
        StorageType.S3,
        testPrincipal,
        "file://a/b/c",
        false,
        new StorageProperties.S3Properties(new AwsCredentials.SimpleAwsCredentials("", "", ""))));
    var providerObj = providerService.createProvider(
        new CreateProvider(providerName, storageObj.name(), Optional.empty(), testPrincipal));
    var tableObj = tableService.createInternalTable(
        providerObj.name(),
        testPrincipal,
        new CreateInternalTable(
            tableName,
            Optional.empty(),
            false,
            new InternalTable.DeltaTableProperties("file://a/b/c/table")));
    return tableObj;
  }
}
