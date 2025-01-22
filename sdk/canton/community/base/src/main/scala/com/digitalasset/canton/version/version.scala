// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.google.protobuf.ByteString

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]
  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  type HasMemoizedProtocolVersionedWrapperCompanion[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = HasMemoizedProtocolVersionedWrapperCompanion2[ValueClass, ValueClass]

  type HasMemoizedProtocolVersionedWithContextCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = HasMemoizedProtocolVersionedWithContextCompanion2WithoutDependency[
    ValueClass,
    ValueClass,
    Context,
    Unit,
  ]

  type HasMemoizedProtocolVersionedWithContextAndDependencyCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      Dependency,
  ] = HasMemoizedProtocolVersionedWithContextCompanion2[ValueClass, ValueClass, Context, Dependency]

  type HasProtocolVersionedWrapperCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ] = HasSupportedProtoVersions[ValueClass, ?, DeserializedValueClass, ?]

  type HasProtocolVersionedCompanion[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = HasProtocolVersionedCompanion2[ValueClass, ValueClass]

  type HasProtocolVersionedWithContextCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = HasProtocolVersionedWithContextCompanion2[ValueClass, ValueClass, Context]
}
