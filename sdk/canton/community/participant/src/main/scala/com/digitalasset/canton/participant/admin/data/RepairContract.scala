// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.implicits.*
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.data.Counter
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.{ByteStringUtil, GrpcStreamingUtils, ResourceUtil}
import com.digitalasset.canton.{LfPackageId, ReassignmentCounter}
import com.digitalasset.daml.lf.transaction.{CreationTime, TransactionCoder}
import com.google.protobuf.ByteString

import java.io.ByteArrayInputStream

/** A contract to add/import with admin repairs.
  */
final case class RepairContract(
    synchronizerId: SynchronizerId,
    contract: LfFatContractInst,
    reassignmentCounter: ReassignmentCounter,
    representativePackageId: LfPackageId,
) {
  def contractId: LfContractId = contract.contractId

  def withContractInstance(
      contract: LfFatContractInst
  ): RepairContract = copy(contract = contract)
}

object RepairContract {

  /** Takes an ACS snapshot that has been created with `export_acs` command and converts to a list
    * of contracts.
    */
  def loadAcsSnapshot(
      acsSnapshot: ByteString
  ): Either[String, List[RepairContract]] =
    for {
      decompressedBytes <-
        ByteStringUtil
          .decompressGzip(acsSnapshot, None)
          .leftMap(err => s"Failed to decompress bytes: $err")
      contracts <- ResourceUtil.withResource(
        new ByteArrayInputStream(decompressedBytes.toByteArray)
      ) { inputSource =>
        GrpcStreamingUtils
          .parseDelimitedFromTrusted[ActiveContract](
            inputSource,
            ActiveContract,
          )
      }
      repairContracts <- contracts.traverse(c => toRepairContract(c.contract))
    } yield repairContracts

  def toRepairContract(contract: LapiActiveContract): Either[String, RepairContract] =
    for {
      event <- Either.fromOption(
        contract.createdEvent,
        "Create node in ActiveContract should not be empty",
      )

      blob = event.createdEventBlob

      fattyContract <- TransactionCoder
        .decodeFatContractInstance(blob)
        .leftMap(decodeError =>
          s"Unable to decode contract event payload: ${decodeError.errorMessage}"
        )

      // TODO(#25385): Assume populated representativePackageId starting with 3.4
      representativePackageId <- Option(event.representativePackageId)
        .filter(_.nonEmpty)
        .traverse(LfPackageId.fromString)
        .leftMap(err => s"Unable to parse representative package id: $err")
        .map(_.getOrElse(fattyContract.templateId.packageId))

      fatContractInstance <- fattyContract.traverseCreateAt {
        case absolute: CreationTime.CreatedAt => Right(absolute)
        case _ => Left("Unable to determine create time.")
      }

      synchronizerId <- SynchronizerId
        .fromString(contract.synchronizerId)
        .leftMap(deserializationError =>
          s"Unable to deserialize synchronized id from ${contract.synchronizerId}: $deserializationError"
        )
    } yield RepairContract(
      synchronizerId = synchronizerId,
      contract = fatContractInstance,
      reassignmentCounter = Counter(contract.reassignmentCounter),
      representativePackageId = representativePackageId,
    )

}
