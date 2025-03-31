// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Represents the contract ID processing mode that should be applied on contract IDs found in an
  * ACS import.
  */
sealed trait ContractIdImportMode extends Product with Serializable with PrettyPrinting {
  def toProtoV30: v30.ContractIdImportMode
}

object ContractIdImportMode {

  case object Accept extends ContractIdImportMode {
    override def toProtoV30: v30.ContractIdImportMode =
      v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_ACCEPT

    override def pretty: Pretty[Accept.type] = prettyOfObject[Accept.type]
  }

  case object Validation extends ContractIdImportMode {
    override def toProtoV30: v30.ContractIdImportMode =
      v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_VALIDATION

    override def pretty: Pretty[Validation.type] = prettyOfObject[Validation.type]
  }

  case object Recomputation extends ContractIdImportMode {
    override def toProtoV30: v30.ContractIdImportMode =
      v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_RECOMPUTATION

    override def pretty: Pretty[Recomputation.type] = prettyOfObject[Recomputation.type]
  }

  def fromProtoV30(
      contractIdReComputationModeP: v30.ContractIdImportMode
  ): ParsingResult[ContractIdImportMode] =
    contractIdReComputationModeP match {
      case v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_UNSPECIFIED =>
        Left(FieldNotSet(contractIdReComputationModeP.name))
      case v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_ACCEPT =>
        Right(Accept)
      case v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_VALIDATION =>
        Right(Validation)
      case v30.ContractIdImportMode.CONTRACT_ID_IMPORT_MODE_RECOMPUTATION =>
        Right(Recomputation)
      case v30.ContractIdImportMode.Unrecognized(value) =>
        Left(UnrecognizedEnum(contractIdReComputationModeP.name, value))
    }
}
