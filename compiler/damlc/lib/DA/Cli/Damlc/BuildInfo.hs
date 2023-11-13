-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Cli.Damlc.BuildInfo
  ( buildInfo
  ) where

import SdkVersion
import Text.PrettyPrint.ANSI.Leijen qualified as PP

buildInfo :: PP.Doc
buildInfo = "SDK Version: " <> PP.text sdkVersion
