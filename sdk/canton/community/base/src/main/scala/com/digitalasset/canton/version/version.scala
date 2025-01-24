// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.google.protobuf.ByteString

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]
  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  // Main use cases
  type VersioningCompanionWithContextMemoization[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContextMemoization2[
    ValueClass,
    ValueClass,
    Context,
    Unit,
  ]

  type VersioningCompanionContextNoMemoization[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContextNoMemoization2[ValueClass, ValueClass, Context]

  type VersioningCompanionNoContextMemoization[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = VersioningCompanionNoContextMemoization2[ValueClass, ValueClass]

  type VersioningCompanionNoContextNoMemoization[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = VersioningCompanionNoContextNoMemoization2[ValueClass, ValueClass]

  // Dependency
  type VersioningCompanionContextMemoizationWithDependency[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      Dependency,
  ] = VersioningCompanionContextMemoization2[ValueClass, ValueClass, Context, Dependency]
}
