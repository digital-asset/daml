// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.ContractIdAbsolutizationData
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}

/** Replaces all relative contract IDs in a contract ID container with their absolutized version
  */
class ContractIdAbsolutizer(
    crypto: HashOps & HmacOps,
    absolutizationData: ContractIdAbsolutizationData,
) {

  def absolutizeContractId(contractId: LfContractId): Either[String, LfContractId] =
    contractId match {
      case v1: LfContractId.V1 => Right(v1)
      case v2: LfContractId.V2 =>
        if (v2.isLocal)
          Left(s"Cannot convert a local contract ID into an absolute contract ID: $v2")
        else if (v2.isAbsolute) Right(v2)
        else {
          // TODO(#23971) implement this
          crypto.discard
          throw new UnsupportedOperationException(
            "Absolutization of relative contract IDs V2 is not yet supported"
          )
        }
    }

  def absolutizeNode(node: LfNode): Either[String, LfNode] =
    node.traverseCid(absolutizeContractId)

  def absolutizeFci(
      fci: FatContractInstance
  ): Either[String, LfFatContractInst] = {
    val absolutizedIdsInArg = Map.newBuilder[LfContractId, LfContractId]
    for {
      absolutizedArg <- fci.createArg.traverseCid { cid =>
        absolutizeContractId(cid).map { absolutized =>
          if (absolutized != cid) {
            absolutizedIdsInArg += cid -> absolutized
          }
          absolutized
        }
      }
      absolutizedKey <- fci.contractKeyWithMaintainers.traverse { keyWithMaintainers =>
        val gkey = keyWithMaintainers.globalKey
        // A contract key normally does not contain contract IDs, so this should be a no-op.
        // However, if it does, we absolutize them too.
        gkey.key.traverseCid(absolutizeContractId).map { absolutizedKey =>
          keyWithMaintainers.copy(globalKey =
            LfGlobalKey.assertBuild(gkey.templateId, absolutizedKey, gkey.packageName)
          )
        }
      }
      absolutizedCid <- absolutizeContractId(fci.contractId)
      absolutizedCreationTime <- absolutizationData.updateLedgerTime(fci.createdAt)
    } yield {
      // TODO(#23971) incorporate absolutized contract IDs in arg into authentication data
      absolutizedIdsInArg.result().discard
      LfFatContractInst.fromCreateNode(
        fci.toCreateNode.copy(
          coid = absolutizedCid,
          arg = absolutizedArg,
          keyOpt = absolutizedKey,
        ),
        absolutizedCreationTime,
        fci.authenticationData,
      )
    }
  }

  def absolutizeTransaction(
      tx: LfVersionedTransaction
  ): Either[String, LfVersionedTransaction] =
    tx.transaction.nodes.toSeq
      .traverse { case (nodeId, node) =>
        absolutizeNode(node).map(nodeId -> _)
      }
      .map { translatedNodes =>
        LfVersionedTransaction(
          tx.version,
          translatedNodes.toMap,
          tx.roots,
        )
      }
}

object ContractIdAbsolutizer {

  sealed trait ContractIdAbsolutizationData extends Product with Serializable {
    def updateLedgerTime(relativeCreationTime: CreationTime): Either[String, CreationTime.CreatedAt]
  }

  case object ContractIdAbsolutizationDataV1 extends ContractIdAbsolutizationData {
    override def updateLedgerTime(
        relativeCreationTime: CreationTime
    ): Either[String, CreationTime.CreatedAt] =
      relativeCreationTime match {
        case absolute: CreationTime.CreatedAt =>
          Right(absolute)
        case _ =>
          Left(s"Invalid creation time for V1 contract ID absolutization: $relativeCreationTime")
      }
  }

  // TODO(#23971) Add V2 contract ID absolutization data
}
