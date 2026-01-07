// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.ContractIdAbsolutizationData
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.SortedSet

/** Replaces all relative contract IDs in a contract ID container with their absolutized version
  */
class ContractIdAbsolutizer(
    hashOps: HashOps,
    absolutizationData: ContractIdAbsolutizationData,
) {

  def absolutizeContractId(contractId: LfContractId): Either[String, LfContractId] =
    absolutizeContractIdInternal(contractId).map(
      _.fold(contractId) { case (_, absolutizedCid) => absolutizedCid }
    )

  private def absolutizeContractIdInternal(
      contractId: LfContractId
  ): Either[String, Option[(RelativeContractIdSuffixV2, LfContractId)]] =
    contractId match {
      case _: LfContractId.V1 => Right(None)
      case v2: LfContractId.V2 =>
        if (v2.isLocal)
          Left(s"Cannot convert a local contract ID into an absolute contract ID: $v2")
        else if (v2.isAbsolute) Right(None)
        else {
          ContractIdAbsolutizer.absoluteSuffixV2(hashOps)(v2, absolutizationData).map(Some(_))
        }
    }

  def absolutizeNode(node: LfNode): Either[String, LfNode] =
    node.traverseCid(absolutizeContractId)

  def absolutizeFci(
      fci: FatContractInstance
  ): Either[String, LfFatContractInst] = {
    val relativeSuffixesInArgBuilder = SortedSet.newBuilder[RelativeContractIdSuffixV2]
    for {
      absolutizedArg <- fci.createArg.traverseCid { cid =>
        absolutizeContractIdInternal(cid).map {
          case None => cid
          case Some((relativeSuffix, absolutizedCid)) =>
            relativeSuffixesInArgBuilder += relativeSuffix
            absolutizedCid
        }
      }
      relativeSuffixesInArg = relativeSuffixesInArgBuilder.result()
      absolutizedKey <- fci.contractKeyWithMaintainers.traverse { keyWithMaintainers =>
        val gkey = keyWithMaintainers.globalKey
        // A contract key normally does not contain contract IDs, so this should be a no-op.
        // However, if it does, we absolutize them too.
        gkey.key
          .traverseCid { cid =>
            absolutizeContractIdInternal(cid).flatMap {
              case None => Right(cid)
              case Some((relativeSuffix, absolutizedCid)) =>
                Either.cond(
                  relativeSuffixesInArg.contains(relativeSuffix),
                  absolutizedCid,
                  s"Relative contract ID $cid appears in the contract key, but not in the contract argument",
                )
            }
          }
          .map { absolutizedKey =>
            keyWithMaintainers.copy(globalKey =
              LfGlobalKey.assertBuild(gkey.templateId, absolutizedKey, gkey.packageName)
            )
          }
      }
      relativeSuffixesInArg = relativeSuffixesInArgBuilder.result()
      absolutizedCid <- absolutizeContractId(fci.contractId)
      absolutizedCreationTime <- absolutizationData.updateLedgerTime(absolutizedCid, fci.createdAt)
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(fci.contractId)
        .leftMap(err => s"Invalid contract ID version: $err")
      authenticationData <- ContractAuthenticationData
        .fromLfBytes(contractIdVersion, fci.authenticationData)
        .leftMap(_.toString)

      absolutizedAuthenticationData <- absolutizationData
        .absolutizeAuthenticationData(relativeSuffixesInArg, authenticationData)
    } yield {
      LfFatContractInst.fromCreateNode(
        fci.toCreateNode.copy(
          coid = absolutizedCid,
          arg = absolutizedArg,
          keyOpt = absolutizedKey,
        ),
        absolutizedCreationTime,
        absolutizedAuthenticationData.toLfBytes,
      )
    }
  }

  def absolutizeTransaction(
      tx: LfVersionedTransaction
  ): Either[String, LfVersionedTransaction] =
    tx.transaction.nodes.toSeq
      .traverse { case (nodeId, node) => absolutizeNode(node).map(nodeId -> _) }
      .map { translatedNodes =>
        LfVersionedTransaction(tx.version, translatedNodes.toMap, tx.roots)
      }

  def absolutizeContractInstance(instance: GenContractInstance): Either[String, ContractInstance] =
    for {
      absolutizedFci <- absolutizeFci(instance.inst)
      absolutizedInstance <- ContractInstance.create(absolutizedFci)
    } yield absolutizedInstance
}

object ContractIdAbsolutizer {

  sealed trait ContractIdAbsolutizationData extends Product with Serializable {
    def updateLedgerTime(
        contractId: LfContractId,
        relativeCreationTime: CreationTime,
    ): Either[String, CreationTime.CreatedAt]
    def absolutizeAuthenticationData(
        relativeSuffixesInArg: SortedSet[RelativeContractIdSuffixV2],
        authenticationData: ContractAuthenticationData,
    ): Either[String, ContractAuthenticationData]
  }

  case object ContractIdAbsolutizationDataV1 extends ContractIdAbsolutizationData {
    override def updateLedgerTime(
        contractId: LfContractId,
        relativeCreationTime: CreationTime,
    ): Either[String, CreationTime.CreatedAt] =
      relativeCreationTime match {
        case absolute: CreationTime.CreatedAt =>
          Right(absolute)
        case _ =>
          Left(
            s"Invalid creation time for V1 contract ID $contractId absolutization: $relativeCreationTime"
          )
      }

    override def absolutizeAuthenticationData(
        relativeSuffixesInArg: SortedSet[RelativeContractIdSuffixV2],
        authenticationData: ContractAuthenticationData,
    ): Either[String, ContractAuthenticationData] =
      Either.cond(
        relativeSuffixesInArg.isEmpty,
        authenticationData,
        s"V1 contract IDs cannot handle absolutization of contract IDs in the contract argument: $relativeSuffixesInArg",
      )
  }

  final case class ContractIdAbsolutizationDataV2(
      creatingUpdateId: UpdateId,
      ledgerTimeOfTx: CantonTimestamp,
  ) extends ContractIdAbsolutizationData {
    override def updateLedgerTime(
        contractId: LfContractId,
        relativeCreationTime: CreationTime,
    ): Either[String, CreationTime.CreatedAt] = relativeCreationTime match {
      case CreationTime.Now => Right(CreationTime.CreatedAt(ledgerTimeOfTx.toLf))
      case createdAt: CreationTime.CreatedAt =>
        Left(
          s"Invalid creation time for V2 contract ID $contractId absolutization: $createdAt, expected 'now'"
        )
    }

    override def absolutizeAuthenticationData(
        relativeSuffixesInArg: SortedSet[RelativeContractIdSuffixV2],
        authenticationData: ContractAuthenticationData,
    ): Either[String, ContractAuthenticationData] = authenticationData match {
      case v2: ContractAuthenticationDataV2 =>
        for {
          _ <-
            v2.creatingUpdateId.traverse_(updateId =>
              Either.left(
                s"Cannot absolutize authentication data that already contains a transaction ID ${updateId.toHexString}"
              )
            )
          _ <- Either.cond(
            v2.relativeArgumentSuffixes.isEmpty,
            (),
            "Cannot absolutize authentication data that already contains relative argument suffixes",
          )
        } yield {
          ContractAuthenticationDataV2(
            v2.salt,
            Some(creatingUpdateId),
            relativeSuffixesInArg.toSeq.map(_.toBytes),
          )(v2.contractIdVersion)
        }
      case _: ContractAuthenticationDataV1 =>
        Left("Cannot absolutize V1 authentication data for a V2 contract ID")
    }
  }

  @VisibleForTesting
  def absoluteSuffixV2(hashOps: HashOps)(
      v2: LfContractId.V2,
      absolutizationData: ContractIdAbsolutizationData,
  ): Either[String, (RelativeContractIdSuffixV2, ContractId.V2)] =
    absolutizationData match {
      case ContractIdAbsolutizationDataV2(updateId, ledgerTime) =>
        for {
          version <- CantonContractIdVersion.extractCantonContractIdVersionV2(v2)
        } yield {
          val hash = hashOps
            .build(HashPurpose.ContractIdAbsolutization)
            .add(v2.suffix.toByteString)
            .add(updateId.getCryptographicEvidence)
            .add(ledgerTime.toProtoPrimitive)
            .finish()
          val suffix = version.versionPrefixBytesAbsolute.toByteString.concat(hash.unwrap)
          RelativeContractIdSuffixV2(version, v2.suffix) ->
            LfContractId.V2(v2.local, Bytes.fromByteString(suffix))
        }
      case ContractIdAbsolutizationDataV1 =>
        Left(
          s"Cannot convert relative contract ID $v2 into absolute contract ID with V1 data"
        )
    }
}
