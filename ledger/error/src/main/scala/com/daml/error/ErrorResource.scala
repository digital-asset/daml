// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  val all = Seq(ContractId, ContractKey, DalfPackage, LedgerId, CommandId, TransactionId, Party)

  def fromString(str: String): Option[ErrorResource] = all.find(_.asString == str)

  object ContractId extends ErrorResource {
    def asString: String = "CONTRACT_ID"
  }
  object ContractKey extends ErrorResource {
    def asString: String = "CONTRACT_KEY"
  }
  object TransactionId extends ErrorResource {
    def asString: String = "TRANSACTION_ID"
  }
  object DalfPackage extends ErrorResource {
    def asString: String = "PACKAGE"
  }
  object TemplateId extends ErrorResource {
    def asString: String = "TEMPLATE_ID"
  }
  object InterfaceId extends ErrorResource {
    def asString: String = "INTERFACE_ID"
  }
  object LedgerId extends ErrorResource {
    def asString: String = "LEDGER_ID"
  }
  object CommandId extends ErrorResource {
    def asString: String = "COMMAND_ID"
  }
  object Party extends ErrorResource {
    def asString: String = "PARTY"
  }
  object User extends ErrorResource {
    def asString: String = "USER"
  }
}
