// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

package object cache {
  import com.daml.lf.value.{Value as lfval}
  private[cache] type ContractId = lfval.ContractId
  private[cache] val ContractId = com.daml.lf.value.Value.ContractId
  private[cache] type Value = lfval.VersionedValue
  private[cache] type Contract = lfval.VersionedContractInstance

  import com.daml.lf.{transaction as lftx}
  private[cache] type Key = lftx.GlobalKey

  import com.daml.lf.{data as lfdata}
  private[cache] type Party = lfdata.Ref.Party
  private[cache] val Party = lfdata.Ref.Party
  private[cache] type Identifier = lfdata.Ref.Identifier
  private[cache] val Identifier = lfdata.Ref.Identifier
}
