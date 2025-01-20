// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ] = HasSupportedProtoVersions[ValueClass, DeserializedValueClass, ?]

  type HasProtocolVersionedCompanion[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = HasProtocolVersionedCompanion2[ValueClass, ValueClass]

  type HasProtocolVersionedWithContextCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = HasProtocolVersionedWithContextCompanion2[ValueClass, ValueClass, Context]
}
