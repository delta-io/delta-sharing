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

package io.delta.sharing.filters

// Prunes UnsupportedOp nodes from a json predicate op tree in a safe manner.
//
// OpConverter emits UnsupportedOp as a placeholder for sub-expressions it cannot convert
// (e.g. nested struct field access). Before the tree is serialized and sent to the server,
// this pruner collapses those nodes so the server only receives well-known op types.
//
// The pruning is conservative -- the returned tree covers a superset of matching data files:
//   AND: an unsupported child is simply dropped (And(A,B,C) is a subset of And(A,B) when C
//        is dropped, so no files are incorrectly skipped).
//   OR:  if any child is unsupported the entire OrOp is dropped (Or(A,B,C) is a superset of
//        Or(A,B) when C is dropped, so we cannot safely prune the branch).
//   NOT: if the child subtree contains any unsupported node the NotOp is dropped
//        (Not(And(A,B,C)) is a superset of Not(And(A,B)) when C is dropped).
object UnsupportedOpPruner {

  // Prune UnsupportedOp nodes from the tree rooted at op.
  // Returns (Some(pruned tree) or None if the entire tree was pruned away,
  //          true if any UnsupportedOp nodes were encountered during pruning).
  def prune(op: BaseOp): (Option[BaseOp], Boolean) = pruneRecurse(op)

  // Returns (pruned op or None, whether any UnsupportedOp was encountered in this subtree).
  private def pruneRecurse(op: BaseOp): (Option[BaseOp], Boolean) = op match {
    case _: UnsupportedOp =>
      (None, true)

    case AndOp(children) =>
      var hadUnsupported = false
      val kept = children.flatMap { child =>
        val (result, unsupported) = pruneRecurse(child)
        if (unsupported) hadUnsupported = true
        result
      }
      kept match {
        case Seq() => (None, hadUnsupported)
        case Seq(only) => (Some(only), hadUnsupported)
        case many => (Some(AndOp(many)), hadUnsupported)
      }

    case OrOp(children) =>
      var hadUnsupported = false
      val kept = children.flatMap { child =>
        val (result, unsupported) = pruneRecurse(child)
        if (unsupported) hadUnsupported = true
        if (result.isEmpty) return (None, true)
        result
      }
      (Some(OrOp(kept)), hadUnsupported)

    case NotOp(children) =>
      var hadUnsupported = false
      val kept = children.flatMap { child =>
        val (result, unsupported) = pruneRecurse(child)
        if (unsupported) hadUnsupported = true
        result
      }
      if (hadUnsupported) (None, true)
      else (Some(NotOp(kept)), false)

    case other =>
      (Some(other), false)
  }
}
