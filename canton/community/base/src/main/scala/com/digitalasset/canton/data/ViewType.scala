// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, ValueConversionError}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RequestProcessor, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasVersionedToByteString

/** Reifies the subclasses of [[ViewTree]] as values */
// This trait does not extend ProtoSerializable because v0.EncryptedViewMessage.ViewType is an enum, not a message.
sealed trait ViewType extends Product with Serializable with PrettyPrinting {

  /** The subclass of [[ViewTree]] that is reified. */
  type View <: ViewTree with HasVersionedToByteString

  type FullView <: ViewTree

  type Processor = RequestProcessor[this.type]

  def toProtoEnum: v30.ViewType

  override def pretty: Pretty[ViewType.this.type] = prettyOfObject[ViewType.this.type]
}

// This trait is not sealed so that we can extend it for unit testing
trait ViewTypeTest extends ViewType

object ViewType {

  def fromProtoEnum: v30.ViewType => ParsingResult[ViewType] = {
    case v30.ViewType.TransactionViewType => Right(TransactionViewType)
    case v30.ViewType.TransferOutViewType => Right(TransferOutViewType)
    case v30.ViewType.TransferInViewType => Right(TransferInViewType)
    case v30.ViewType.MissingViewType => Left(FieldNotSet("viewType"))
    case v30.ViewType.Unrecognized(value) =>
      Left(ValueConversionError("viewType", s"Unrecognized value $value"))
  }

  case object TransactionViewType extends ViewType {
    override type View = LightTransactionViewTree

    override type FullView = FullTransactionViewTree

    override def toProtoEnum: v30.ViewType = v30.ViewType.TransactionViewType
  }
  type TransactionViewType = TransactionViewType.type

  sealed trait TransferViewType extends ViewType {
    type View <: TransferViewTree with HasVersionedToByteString
    type FullView = View
  }

  case object TransferOutViewType extends TransferViewType {
    override type View = FullTransferOutTree
    override def toProtoEnum: v30.ViewType = v30.ViewType.TransferOutViewType
  }
  type TransferOutViewType = TransferOutViewType.type

  case object TransferInViewType extends TransferViewType {
    override type View = FullTransferInTree
    override def toProtoEnum: v30.ViewType = v30.ViewType.TransferInViewType
  }
  type TransferInViewType = TransferInViewType.type
}
