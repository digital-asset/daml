// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  val all = Seq(
    CommandId,
    ContractArg,
    ContractId,
    ContractIds,
    ContractKey,
    ContractKeyHash,
    DalfPackage,
    ExceptionType,
    ExceptionValue,
    IdentityProviderConfig,
    InterfaceId,
    LedgerId,
    Parties,
    Party,
    TemplateId,
    TransactionId,
    User,
  )

  def fromString(str: String): Option[ErrorResource] = all.find(_.asString == str)

  object ContractId extends ErrorResource {
    def asString: String = "CONTRACT_ID"
  }
  object ContractIds extends ErrorResource {
    def asString: String = "CONTRACT_IDS"
  }
  object ContractKey extends ErrorResource {
    def asString: String = "CONTRACT_KEY"
  }
  object ContractArg extends ErrorResource {
    def asString: String = "CONTRACT_ARG"
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
  object Parties extends ErrorResource {
    def asString: String = "PARTIES"
  }
  object User extends ErrorResource {
    def asString: String = "USER"
  }
  object IdentityProviderConfig extends ErrorResource {
    def asString: String = "IDENTITY_PROVIDER_CONFIG"
  }
  object ContractKeyHash extends ErrorResource {
    def asString: String = "CONTRACT_KEY_HASH"
  }
  object ExceptionValue extends ErrorResource {
    def asString: String = "EXCEPTION_VALUE"
  }
  object ExceptionType extends ErrorResource {
    def asString: String = "EXCEPTION_TYPE"
  }
  object ExceptionText extends ErrorResource {
    def asString: String = "EXCEPTION_TEXT"
  }
  object DevErrorType extends ErrorResource {
    def asString: String = "DEV_ERROR_TYPE"
  }
}
