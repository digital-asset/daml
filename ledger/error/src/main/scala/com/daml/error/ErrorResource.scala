// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** Type of error resource
  *
  * Some errors are linked to a specific resource such as a contract id or a package id.
  * In such cases, we include the resource identifier as part of the error message.
  * This enum allows an error to provide identifiers of a resource
  */
trait ErrorResource {
  def asString: String
}

object ErrorResource {

  private lazy val all =
    Seq(ContractId, ContractKey, DalfPackage, LedgerId, CommandId)

  def fromString(str: String): Option[ErrorResource] = all.find(_.asString == str)

  object ContractId extends ErrorResource {
    def asString: String = "CONTRACT_ID"
  }
  object ContractKey extends ErrorResource {
    def asString: String = "CONTRACT_KEY"
  }
  object DalfPackage extends ErrorResource {
    def asString: String = "PACKAGE"
  }
  object LedgerId extends ErrorResource {
    def asString: String = "LEDGER_ID"
  }
  object CommandId extends ErrorResource {
    def asString: String = "COMMAND_ID"
  }
}
