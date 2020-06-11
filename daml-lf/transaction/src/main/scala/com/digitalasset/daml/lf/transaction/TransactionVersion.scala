// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.transaction.{Transaction => Tx}
import com.daml.lf.value.{Value, ValueVersion}

final case class TransactionVersion(protoValue: String)

/**
  * Currently supported versions of the DAML-LF transaction specification.
  */
object TransactionVersions
    extends LfVersions(versionsAscending = VersionTimeline.ascendingVersions[TransactionVersion])(
      _.protoValue,
    ) {

  private[this] val minVersion = TransactionVersion("1")
  private[transaction] val minKeyOrLookupByKey = TransactionVersion("3")
  private[transaction] val minFetchActors = TransactionVersion("5")
  private[transaction] val minNoControllers = TransactionVersion("6")
  private[transaction] val minExerciseResult = TransactionVersion("7")
  private[transaction] val minContractKeyInExercise = TransactionVersion("8")
  private[transaction] val minMaintainersInExercise = TransactionVersion("9")
  private[transaction] val minContractKeyInFetch = TransactionVersion("10")

  // Older versions are deprecated https://github.com/digital-asset/daml/issues/5220
  // We force output of recent version, but keep reading older version as long as
  // Sandbox is alive.
  private[transaction] val minOutputVersion = TransactionVersion("10")

  def assignVersion(
      a: GenTransaction.WithTxValue[_, Value.ContractId],
  ): TransactionVersion = {
    require(a != null)
    import VersionTimeline.Implicits._

    VersionTimeline.latestWhenAllPresent(
      minOutputVersion,
      // latest version used by any value
      a.foldValues(ValueVersion("1")) { (z, vv) =>
        VersionTimeline.maxVersion(z, vv.version)
      },
      // a NodeCreate with defined `key` or a NodeLookupByKey
      // implies minimum version 3
      if (a.nodes.values.exists {
          case nc: Node.NodeCreate[_, _] => nc.key.isDefined
          case _: Node.NodeLookupByKey[_, _] => true
          case _: Node.NodeFetch[_, _] | _: Node.NodeExercises[_, _, _] => false
        }) minKeyOrLookupByKey
      else
        minVersion,
      // a NodeFetch with actingParties implies minimum version 5
      if (a.nodes.values
          .exists { case nf: Node.NodeFetch[_, _] => nf.actingParties.nonEmpty; case _ => false })
        minFetchActors
      else
        minVersion,
      if (a.nodes.values
          .exists {
            case ne: Node.NodeExercises[_, _, _] => ne.exerciseResult.isDefined
            case _ => false
          })
        minExerciseResult
      else
        minVersion,
      if (a.nodes.values
          .exists {
            case ne: Node.NodeExercises[_, _, _] => ne.key.isDefined
            case _ => false
          })
        minContractKeyInExercise
      else
        minVersion,
      if (a.nodes.values
          .exists {
            case ne: Node.NodeExercises[_, _, _] =>
              ne.key match {
                case Some(KeyWithMaintainers(key @ _, maintainers)) => maintainers.nonEmpty
                case _ => false
              }
            case _ => false
          })
        minMaintainersInExercise
      else
        minVersion,
      if (a.nodes.values
          .exists {
            case nf: Node.NodeFetch[_, _] => nf.key.isDefined
            case _ => false
          })
        minContractKeyInFetch
      else
        minVersion,
    )
  }

  def asVersionedTransaction(
      tx: GenTransaction.WithTxValue[Tx.NodeId, Value.ContractId],
  ): Either[String, Tx.Transaction] =
    Right(VersionedTransaction(assignVersion(tx), tx))

  @throws[IllegalArgumentException]
  def assertAsVersionedTransaction[Nid, Cid](
      tx: GenTransaction.WithTxValue[Tx.NodeId, Value.ContractId],
  ): Tx.Transaction =
    data.assertRight(asVersionedTransaction(tx))

}
