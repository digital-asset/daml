// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.conf

import java.nio.file.Path

final case class JavaConf(damlFilePath: Path, outputDirPath: Path)
