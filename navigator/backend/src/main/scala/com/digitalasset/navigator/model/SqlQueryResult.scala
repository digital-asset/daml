// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model

final case class SqlQueryResult(columnNames: List[String], rows: List[List[String]])
