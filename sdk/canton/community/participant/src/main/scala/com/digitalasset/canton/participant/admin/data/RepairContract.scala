// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.implicits.*
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.{ByteStringUtil, GrpcStreamingUtils, ResourceUtil}
import com.digitalasset.canton.{LfVersioned, ReassignmentCounter}
import com.digitalasset.daml.lf.transaction
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.google.protobuf.ByteString

import java.io.ByteArrayInputStream

/** A contract to add/import with admin repairs.
  */
final case class RepairContract(
    synchronizerId: SynchronizerId,
    contract: SerializableContract,
    reassignmentCounter: ReassignmentCounter,
) {
  def contractId: LfContractId = contract.contractId

  def withSerializableContract(
      contract: SerializableContract
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

      contractInstance = LfThinContractInst(
        fattyContract.packageName,
        fattyContract.templateId,
        transaction.Versioned(fattyContract.version, fattyContract.createArg),
      )

      maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]] =
        fattyContract.contractKeyWithMaintainers.map { value =>
          transaction.Versioned(
            fattyContract.version,
            LfGlobalKeyWithMaintainers(value.globalKey, value.maintainers),
          )
        }

      rawContractInstance <- SerializableRawContractInstance
        .create(contractInstance)
        .leftMap(encodeError => s"Unable to encode contract instance: ${encodeError.errorMessage}")

      contractMetadata <- ContractMetadata.create(
        signatories = fattyContract.signatories,
        stakeholders = fattyContract.stakeholders,
        maybeKeyWithMaintainersVersioned,
      )

      ledgerCreateTime = LedgerCreateTime(CantonTimestamp(fattyContract.createdAt))

      driverContractMetadata <-
        DriverContractMetadata
          .fromTrustedByteString(fattyContract.cantonData.toByteString)
          .leftMap(deserializationError =>
            s"Unable to deserialize driver contract metadata: ${deserializationError.message}"
          )

      contractSalt = driverContractMetadata.salt

      serializableContract = new SerializableContract(
        fattyContract.contractId,
        rawContractInstance,
        contractMetadata,
        ledgerCreateTime,
        contractSalt,
      )

      synchronizerId <- SynchronizerId
        .fromString(contract.synchronizerId)
        .leftMap(deserializationError =>
          s"Unable to deserialize synchronized id from ${contract.synchronizerId}: $deserializationError"
        )
    } yield RepairContract(
      synchronizerId,
      serializableContract,
      Counter(contract.reassignmentCounter),
    )

}
