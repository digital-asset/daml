// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

private[codegen] object GenHelper {

  /** Header for a commonjs module. This matches what the typescript compiler would also emit.
    */
  val commonjsHeader: String =
    """|"use strict";
       |/* eslint-disable-next-line no-unused-vars */
       |function __export(m) {
       |/* eslint-disable-next-line no-prototype-builtins */
       |    for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
       |}
       |Object.defineProperty(exports, "__esModule", { value: true });
       |""".stripMargin

  def renderES6Import(ref: String, path: String): String =
    s"import * as $ref from '$path';"

  def renderES5Import(ref: String, path: String): String =
    s"var $ref = require('$path');"
}
