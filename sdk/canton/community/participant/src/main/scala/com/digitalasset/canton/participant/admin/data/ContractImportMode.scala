// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Represents the contract processing mode that should be applied on contracts found in an ACS
  * import.
  */
sealed trait ContractImportMode extends Product with Serializable with PrettyPrinting {
  def toProtoV30: v30.ContractImportMode
}

object ContractImportMode {

  case object Accept extends ContractImportMode {
    override def toProtoV30: v30.ContractImportMode =
      v30.ContractImportMode.CONTRACT_IMPORT_MODE_ACCEPT

    override def pretty: Pretty[Accept.type] = prettyOfObject[Accept.type]
  }

  case object Validation extends ContractImportMode {
    override def toProtoV30: v30.ContractImportMode =
      v30.ContractImportMode.CONTRACT_IMPORT_MODE_VALIDATION

    override def pretty: Pretty[Validation.type] = prettyOfObject[Validation.type]
  }

  case object Recomputation extends ContractImportMode {
    override def toProtoV30: v30.ContractImportMode =
      v30.ContractImportMode.CONTRACT_IMPORT_MODE_RECOMPUTATION

    override def pretty: Pretty[Recomputation.type] = prettyOfObject[Recomputation.type]
  }

  def fromProtoV30(
      contractIdReComputationModeP: v30.ContractImportMode
  ): ParsingResult[ContractImportMode] =
    contractIdReComputationModeP match {
      case v30.ContractImportMode.CONTRACT_IMPORT_MODE_UNSPECIFIED =>
        Left(FieldNotSet(contractIdReComputationModeP.name))
      case v30.ContractImportMode.CONTRACT_IMPORT_MODE_ACCEPT =>
        Right(Accept)
      case v30.ContractImportMode.CONTRACT_IMPORT_MODE_VALIDATION =>
        Right(Validation)
      case v30.ContractImportMode.CONTRACT_IMPORT_MODE_RECOMPUTATION =>
        Right(Recomputation)
      case v30.ContractImportMode.Unrecognized(value) =>
        Left(UnrecognizedEnum(contractIdReComputationModeP.name, value))
    }
}
