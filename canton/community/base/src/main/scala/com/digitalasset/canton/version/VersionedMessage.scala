// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.google.protobuf.ByteString

object VersionedMessage {
  def apply[M](bytes: ByteString, version: Int): VersionedMessage[M] =
    VersionedMessage(
      v1.UntypedVersionedMessage(v1.UntypedVersionedMessage.Wrapper.Data(bytes), version)
    )

  def apply[M](message: v1.UntypedVersionedMessage): VersionedMessage[M] =
    VersionedMessageImpl.Instance.subst(message)
}

sealed abstract class VersionedMessageImpl {
  type VersionedMessage[+A] <: v1.UntypedVersionedMessage

  private[version] def subst[M](message: v1.UntypedVersionedMessage): VersionedMessage[M]
}

object VersionedMessageImpl {
  val Instance: VersionedMessageImpl = new VersionedMessageImpl {
    override type VersionedMessage[+A] = v1.UntypedVersionedMessage

    override private[version] def subst[M](
        message: v1.UntypedVersionedMessage
    ): VersionedMessage[M] =
      message
  }
}
