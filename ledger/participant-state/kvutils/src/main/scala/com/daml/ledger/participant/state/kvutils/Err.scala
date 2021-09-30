// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlState.DamlStateKey
import com.daml.lf.data.Ref

/** Errors thrown by kvutils.
  *
  * Validation and consistency errors are turned into command rejections.
  * Note that [[KeyValueCommitting.processSubmission]] can also fail with a protobuf exception,
  * e.g. https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/InvalidProtocolBufferException.
  */
sealed abstract class Err extends RuntimeException with Product with Serializable {
  def getMetadata: Map[String, String]
}

object Err {
  final case class InvalidSubmission(message: String) extends Err {
    override def getMessage: String = s"Invalid submission: $message"

    override def getMetadata: Map[String, String] = Map.empty
  }

  final case class MissingInputState(key: DamlStateKey) extends Err {
    override def getMessage: String =
      s"Missing input state for key $key. Hint: the referenced contract might have been archived."

    override def getMetadata: Map[String, String] = Map("key" -> key.toString)
  }

  final case class ArchiveDecodingFailed(packageId: Ref.PackageId, reason: String) extends Err {
    override def getMessage: String = s"Decoding of Daml-LF archive $packageId failed: $reason"

    override def getMetadata: Map[String, String] = Map("package_id" -> packageId)
  }

  final case class DecodeError(kind: String, message: String) extends Err {
    override def getMessage: String = s"Decoding $kind failed: $message"

    override def getMetadata: Map[String, String] = Map.empty
  }

  final case class EncodeError(kind: String, message: String) extends Err {
    override def getMessage: String = s"Encoding $kind failed: $message"

    override def getMetadata: Map[String, String] = Map.empty
  }

  final case class InternalError(message: String) extends Err {
    override def getMessage: String = s"Internal error: $message"

    override def getMetadata: Map[String, String] = Map.empty
  }

  final case class MissingDivulgedContractInstance(contractId: String) extends Err {
    override def getMessage: String =
      s"Missing divulged contract instance for contract id $contractId"

    override def getMetadata: Map[String, String] = Map("contract_id" -> contractId)
  }
}
