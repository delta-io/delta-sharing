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

package io.delta.sharing.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.test.SharedSparkSession

class RemoteLogicalRelationSuite extends SharedSparkSession with DeltaSharingIntegrationTest {
  integrationTest("RemoteLogicalRelation test") {
    val cdfOptions = Map("startingVersion" -> "0", "endingVersion" -> "3")
    val path = s"${testProfileFile.getCanonicalPath}#share1.default.cdf_table_cdf_enabled"
    val relation = RemoteLogicalRelation(path, cdfOptions)
    assert(relation.relation.isInstanceOf[RemoteDeltaCDFRelation])
    assert(relation.relation.asInstanceOf[RemoteDeltaCDFRelation].cdfOptions == cdfOptions)
    assert(relation.output.size == 6)
    assert(relation.output(0).name == "name")
    assert(relation.output(1).name == "age")
    assert(relation.output(2).name == "birthday")
    assert(relation.output(3).name == "_commit_version")
    assert(relation.output(4).name == "_commit_timestamp")
    assert(relation.output(5).name == "_change_type")
  }
}
