// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]

  type HasMemoizedProtocolVersionedWrapperCompanion[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = HasMemoizedProtocolVersionedWrapperCompanion2[ValueClass, ValueClass]

  type HasMemoizedProtocolVersionedWithContextCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = HasMemoizedProtocolVersionedWithContextCompanion2[ValueClass, ValueClass, Context]

  type HasProtocolVersionedCompanion[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = HasProtocolVersionedCompanion2[ValueClass, ValueClass]

}
