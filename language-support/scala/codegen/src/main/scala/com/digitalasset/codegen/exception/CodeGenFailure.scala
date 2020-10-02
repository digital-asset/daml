// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.exception

/** Root exception for the codegen */
sealed abstract class CodeGenException(error: String) extends RuntimeException(error)

/** Error while decoding the package or extracting the interface */
final case class PackageInterfaceException(error: String) extends CodeGenException(error)

/** An unsupported DAML type has been found in a template */
final case class UnsupportedDamlTypeException(typeName: String)
    extends CodeGenException(s"Unsupported daml type '$typeName'")

final case class EmptyTemplateException(typeName: String)
    extends CodeGenException(s"template with no arguments found, skipping it $typeName")
