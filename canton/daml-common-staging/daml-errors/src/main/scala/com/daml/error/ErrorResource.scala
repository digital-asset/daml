// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** Type of error resource
  *
  * Some errors are linked to a specific resource such as a contract id or a package id.
  * In such cases, we include the resource identifier as part of the error message.
  * This enum allows an error to provide identifiers of a resource
  */
final case class ErrorResource(asString: String)

object ErrorResource {
  lazy val ContractId: ErrorResource = ErrorResource("CONTRACT_ID")
  lazy val ContractIds: ErrorResource = ErrorResource("CONTRACT_IDS")
  lazy val ContractKey: ErrorResource = ErrorResource("CONTRACT_KEY")
  lazy val ContractArg: ErrorResource = ErrorResource("CONTRACT_ARG")
  lazy val TransactionId: ErrorResource = ErrorResource("TRANSACTION_ID")
  lazy val DalfPackage: ErrorResource = ErrorResource("PACKAGE")
  lazy val TemplateId: ErrorResource = ErrorResource("TEMPLATE_ID")
  lazy val InterfaceId: ErrorResource = ErrorResource("INTERFACE_ID")
  lazy val LedgerId: ErrorResource = ErrorResource("LEDGER_ID")
  lazy val PackageName: ErrorResource = ErrorResource("PACKAGE_NAME")
  lazy val CommandId: ErrorResource = ErrorResource("COMMAND_ID")
  lazy val Party: ErrorResource = ErrorResource("PARTY")
  lazy val Parties: ErrorResource = ErrorResource("PARTIES")
  lazy val User: ErrorResource = ErrorResource("USER")
  lazy val IdentityProviderConfig: ErrorResource = ErrorResource("IDENTITY_PROVIDER_CONFIG")
  lazy val ContractKeyHash: ErrorResource = ErrorResource("CONTRACT_KEY_HASH")
  lazy val ExceptionValue: ErrorResource = ErrorResource("EXCEPTION_VALUE")
  lazy val ExceptionType: ErrorResource = ErrorResource("EXCEPTION_TYPE")
  lazy val ExceptionText: ErrorResource = ErrorResource("EXCEPTION_TEXT")
  lazy val DevErrorType: ErrorResource = ErrorResource("DEV_ERROR_TYPE")
  lazy val DomainId: ErrorResource = ErrorResource("DOMAIN_ID")
  lazy val DomainAlias: ErrorResource = ErrorResource("DOMAIN_ALIAS")

  lazy val all = Seq(
    CommandId,
    ContractArg,
    ContractId,
    ContractIds,
    ContractKey,
    ContractKeyHash,
    DalfPackage,
    DevErrorType,
    ExceptionText,
    ExceptionType,
    ExceptionValue,
    IdentityProviderConfig,
    InterfaceId,
    LedgerId,
    PackageName,
    Parties,
    Party,
    TemplateId,
    TransactionId,
    User,
    DomainId,
    DomainAlias,
  )

  def fromString(str: String): Option[ErrorResource] = all.find(_.asString == str)
}
