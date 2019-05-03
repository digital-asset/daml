// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.LfVersions
import com.digitalasset.daml.lf.value.ValueVersion
import value.Value.VersionedValue

final case class TransactionVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF transaction specification.
  */
object TransactionVersions
    extends LfVersions(
      maxVersion = TransactionVersion("6"),
      previousVersions = List("1", "2", "3", "4", "5") map TransactionVersion)(_.protoValue) {

  private[this] val minVersion = TransactionVersion("1")
  private[transaction] val minKeyOrLookupByKey = TransactionVersion("3")
  private[transaction] val minFetchActors = TransactionVersion("5")
  private[transaction] val minNoControllers = TransactionVersion("6")

  def assignVersion(a: GenTransaction[_, _, _ <: VersionedValue[_]]): TransactionVersion = {
    require(a != null)
    import VersionTimeline.Implicits._
    VersionTimeline.latestWhenAllPresent(
      minVersion,
      // latest version used by any value
      a.foldValues(ValueVersion("1")) { (z, vv) =>
        VersionTimeline.maxVersion(z, vv.version)
      },
      // a NodeCreate with defined `key` or a NodeLookupByKey
      // implies minimum version 3
      if (a.nodes.values.exists {
          case nc: Node.NodeCreate[_, _] => nc.key.isDefined
          case _: Node.NodeLookupByKey[_, _] => true
          case _: Node.NodeFetch[_] | _: Node.NodeExercises[_, _, _] => false
        }) minKeyOrLookupByKey
      else minVersion,
      // a NodeFetch with actingParties implies minimum version 5
      if (a.nodes.values
          .exists { case nf: Node.NodeFetch[_] => nf.actingParties.nonEmpty; case _ => false })
        minFetchActors
      else minVersion,
    )
  }
}
