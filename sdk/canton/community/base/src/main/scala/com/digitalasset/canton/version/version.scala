// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.google.protobuf.ByteString

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]
  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  // Main use cases
  type VersioningCompanionContextMemoization[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContextMemoization2[
    ValueClass,
    Context,
    ValueClass,
    Unit,
  ]

  type VersioningCompanionContext[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContext2[ValueClass, ValueClass, Context]

  type VersioningCompanionMemoization[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = VersioningCompanionMemoization2[ValueClass, ValueClass]

  type VersioningCompanion[ValueClass <: HasRepresentativeProtocolVersion] =
    VersioningCompanion2[ValueClass, ValueClass]

  // Dependency
  type VersioningCompanionContextMemoizationWithDependency[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      Dependency,
  ] = VersioningCompanionContextMemoization2[ValueClass, Context, ValueClass, Dependency]
}
