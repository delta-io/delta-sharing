/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.common

import io.delta.sharing.server.DeltaSharingUnsupportedOperationException
import io.delta.sharing.server.actions.{ColumnMappingTableFeature, DeltaAction}


object SnapshotChecker {

  /**
   * Assert all properties present in the table are covered/supported either by clientReaderFeatures
   * or flagReaderFeatures.
   *
   * If a table property is covered by clientReaderFeatures or flagReaderFeatures, then it's
   * considered as supported, if not, check if the property value is disabled,
   * if not it's considered unsupported, and should throw an error.
   *
   * This should fail all parquet format requests on tables with property values not in
   * tablePropertiesWithDisabledValues, because clientReaderFeatures is empty for parquet format
   * sharing rpcs, which won't filter out any properties in tablePropertiesWithDisabledValues.
   *
   * @param configuration The "configuration" field of a delta Metadata, which contains the
   *                      properties of the table.
   * @param tableVersion  The table vesrion associated with the configuration.
   * @param clientReaderFeatures The set of reader features supported by the delta sharing client
   */
  def assertTableProperties(
      configuration: Map[String, String],
      tableVersion: Option[Long],
      clientReaderFeatures: Set[String]): Unit = {
    // An unsupported table property can be supported if it is in part of the client supported
    // table features.
    def propertySupportedByClient(property: String): Boolean = {
      // TODO: @pranavsuku-db add deletionvector logic
      if (property == DeltaAction.columnMappingProperty.property) {
        ColumnMappingTableFeature.isInSet(clientReaderFeatures)
      } else {
        // We should not reject any other table properties as they can contain arbitrary keys.
        true
      }
    }
    val unsupportedPropertiesByClient = DeltaAction.tablePropertiesWithDisabledValues
      .flatMap {
        case pr @ DeltaAction.PropertyAllowedValues(property, allowedValues)
          if !propertySupportedByClient(property) =>
          configuration.get(property).filterNot(allowedValues.contains(_)).map(_ => pr)
        case _ => None
      }
    if (unsupportedPropertiesByClient.nonEmpty) {
      throw new DeltaSharingUnsupportedOperationException("Unsupported Delta Table Properties")
    }
  }

}
