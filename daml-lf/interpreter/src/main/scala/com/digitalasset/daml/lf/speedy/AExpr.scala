// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**
  * The even more simplified AST for the speedy interpreter, following ANF transformation.
  */
final case class AExpr(wrapped: SExpr) extends Serializable {}
