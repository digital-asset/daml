// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger
package api.internal

import com.digitalasset.daml.lf.ledger.api.internal.host.{ByteString => HostByteString}
import com.google.protobuf.{ByteString => ProtoByteString}
import org.teavm.interop.{Address, Import}

package object host {
  @Import(module = "env", name = "logInfo")
  @native private def logInfo(msg: Address): Unit

  @Import(module = "env", name = "createContract")
  @native private def createContract(templateId: Address, arg: Address): Address

  @Import(module = "env", name = "fetchContractArg")
  @native private def fetchContractArg(templateId: Address, contractId: Address): Address

  @Import(module = "env", name = "exerciseChoice")
  @native private def exerciseChoice(
      templateId: Address,
      contractId: Address,
      choiceName: Address,
      choiceArg: Address,
  ): Address

  private[ledger] def logInfo(msg: String): Unit = {
    logInfo(Address.ofObject(HostByteString(msg)))
  }

  private[internal] def createContract(templateId: Array[Byte], arg: Array[Byte]): Array[Byte] = {
    createContract(
      Address.ofObject(HostByteString(templateId)),
      Address.ofObject(HostByteString(arg)),
    ).toStructure[HostByteString].toByteArray
  }

  private[internal] def fetchContractArg(
      templateId: Array[Byte],
      contractId: Array[Byte],
  ): Array[Byte] = {
    fetchContractArg(
      Address.ofObject(HostByteString(templateId)),
      Address.ofObject(HostByteString(contractId)),
    ).toStructure[HostByteString].toByteArray
  }

  private[internal] def exerciseChoice(
      templateId: Array[Byte],
      contractId: Array[Byte],
      choiceName: String,
      choiceArg: Array[Byte],
  ): Array[Byte] = {
    exerciseChoice(
      Address.ofObject(HostByteString(templateId)),
      Address.ofObject(HostByteString(contractId)),
      Address.ofObject(HostByteString(choiceName)),
      Address.ofObject(HostByteString(choiceArg)),
    ).toStructure[HostByteString].toByteArray
  }
}
