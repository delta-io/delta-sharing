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

package io.delta.kernelsharedtable

// Performs pruning of Json predicate op tree, and removes the parts
// that are not supported in a safe manner.
//
// This is needed to support partial filtering, which ensures that we can
// perfrom as much data skipping as possible in the presence of unsupported
// columns types.
//
// The pruning is "safe" as the returned tree is guaranteed to cover a superset
// of data files compared to the original predicate tree.
// For additional details, see:
//   https://docs.google.com/document/d/
//     1PL_ky1slLK7E5QLdyGJ7ybYh0qTCku25woTNhu-CVkw/edit#bookmark=id.cdu9a4keqbub
//
// param @allowedColums:
//   The set of column names that are allowed to be in the predicate tree.
//   Predicate pushdown into checkpoint reader only supports predicates on
//   partition columns, so we use this to prune away predicates on non-partition
//   columns for that use case.
//   If allowedColums is empty, it implies that this constraint is off and all
//   columns are allowed.
class JsonPredicatePruner(allowedColums: Set[String] = Set.empty) {

  // The API to prune the json predicate tree rooted at the specified op.
  // This is a wrapper on pruneRecurse which does the main work.
  //
  // Returns the resulting pruned op, or None if the entire tree is pruned away
  // in which case the filtering will be skipped.
  //
  // The pruning is "safe" as the returned tree is guaranteed to cover a superset
  // of data files compared to the original predicate tree.
  def prune(op: BaseOp): Option[BaseOp] = {
    val (newOp, _) = pruneRecurse(op)
    newOp
  }

  // Descends down the json predicate expression tree rooted at the specified op
  // and prunes away sub-trees as needed using a post-order DFS strategy.
  //
  // Returns a pair of values:
  //  - The pruned op which is a replacement of the input op.
  //  - A boolean which is true if any descendent was actually pruned.
  //    This is used by operations such as NotOp to make pruning conservative.
  private def pruneRecurse(op: BaseOp): (Option[BaseOp], Boolean) = {
    op match {
      // Prune boolean logic operators.
      case AndOp(children) => pruneAndOp(children)
      case OrOp(children) => pruneOrOp(children)
      case NotOp(children) => pruneNotOp(children)

      // Prune the rest of the operators.
      case _ => pruneDefault(op)
    }
  }

  // Prunes an AndOp using the following strategy:
  //   - If any of children was pruned away, remove it.
  //     It is safe to do so: And(A,B,C) is a subset of And(A,B) if C was pruned.
  //   - If there is only one child left, replace the AndOp with it.
  // Returns a pair of values:
  //  - The pruned op which is a replacement of the input op.
  //  - A boolean which is true if any descendent was actually pruned.
  private def pruneAndOp(children: Seq[BaseOp]): (Option[BaseOp], Boolean) = {
    var hasPrunedDescendent = false
    val prunedChildren = children.flatMap(op => {
      val (newOp, newOpHasPrunedDescendent) = pruneRecurse(op)
      if (newOpHasPrunedDescendent) {
        hasPrunedDescendent = true
      }
      newOp
    })
    prunedChildren match {
      case Seq() => (None, hasPrunedDescendent)
      case Seq(child) => (Some(child), hasPrunedDescendent)
      case children => (Some(AndOp(children)), hasPrunedDescendent)
    }
  }

  // Prunes an OrOp using the following strategy:
  //   - If any of the children was pruned away, prune the OrOp.
  //     We have to do this since: Or(A,B,C) is a superset of Or(A,B) is C was pruned.
  // Returns a pair of values:
  //  - The pruned op which is a replacement of the input op.
  //  - A boolean which is true if any descendent was actually pruned.
  private def pruneOrOp(children: Seq[BaseOp]): (Option[BaseOp], Boolean) = {
    var hasPrunedDescendent = false
    val filteredChildren = children.flatMap(op => {
      val (newOp, newOpHasPrunedDescendent) = pruneRecurse(op)
      if (newOpHasPrunedDescendent) {
        hasPrunedDescendent = true
      }
      if (newOp.isEmpty) {
        return (None, true)
      }
      newOp
    })
    (Some(OrOp(filteredChildren)), hasPrunedDescendent)
  }

  // Prunes a NotOp using the following strategy:
  //   - If any descendent was pruned away, prune the NotOp.
  //     This is needed to avoid interaction between And and Not.
  //     For example, Not(And(A,B,C)) is a superset of Not(And(A, B)) if C was pruned.
  // Returns a pair of values:
  //  - The pruned op which is a replacement of the input op.
  //  - A boolean which is true if any descendent was actually pruned.
  private def pruneNotOp(children: Seq[BaseOp]): (Option[BaseOp], Boolean) = {
    var hasPrunedDescendent = false
    val filteredChildren = children.flatMap(op => {
      val (newOp, newOpHasPrunedDescendent) = pruneRecurse(op)
      if (newOpHasPrunedDescendent) {
        hasPrunedDescendent = true
      }
      newOp
    })
    if (hasPrunedDescendent) {
      (None, true)
    } else {
      (Some(NotOp(filteredChildren)), false)
    }
  }

  // The default pruning operation which uses the following strategy:
  //   - Prune the op if validations fail.
  //   - Prune ColumnOp based on the specified allow list.
  //   - Prune op if any of its children are pruned away.
  // Returns a pair of values:
  //  - The pruned op which is a replacement of the input op.
  //  - A boolean which is true if any descendent was actually pruned.
  private def pruneDefault(op: BaseOp): (Option[BaseOp], Boolean) = {
    //   If allowedColums is empty, it implies that all columns are allowed.
    if (!allowedColums.isEmpty && op.isInstanceOf[ColumnOp] &&
      !allowedColums.contains(op.asInstanceOf[ColumnOp].name)) {
      return (None, true)
    }

    // The Column ops used in comparison ops may get pruned away.
    // We prune the comparison op in this case.
    op.getAllChildren()
      .foreach(child => {
        val (_, childHasPrunedDescendent) = pruneRecurse(child)
        if (childHasPrunedDescendent) {
          return (None, true)
        }
      })

    try {
      op.validate()
      (Some(op), false)
    } catch {
      case e: Exception =>
        (None, true)
    }
  }
}
