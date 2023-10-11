// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.google.protobuf.ByteString

object VersionedMessage {
  def apply[M](bytes: ByteString, version: Int): VersionedMessage[M] =
    VersionedMessage(UntypedVersionedMessage(UntypedVersionedMessage.Wrapper.Data(bytes), version))

  def apply[M](message: UntypedVersionedMessage): VersionedMessage[M] =
    VersionedMessageImpl.Instance.subst(message)
}

sealed abstract class VersionedMessageImpl {
  type VersionedMessage[+A] <: UntypedVersionedMessage

  private[version] def subst[M](message: UntypedVersionedMessage): VersionedMessage[M]
}

object VersionedMessageImpl {
  val Instance: VersionedMessageImpl = new VersionedMessageImpl {
    override type VersionedMessage[+A] = UntypedVersionedMessage

    override private[version] def subst[M](message: UntypedVersionedMessage): VersionedMessage[M] =
      message
  }
}
