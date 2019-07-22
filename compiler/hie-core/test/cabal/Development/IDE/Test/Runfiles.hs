-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Test.Runfiles where

import System.FilePath (FilePath)


locateHieCoreExecutable :: IO FilePath
locateHieCoreExecutable = pure "hie-core"
