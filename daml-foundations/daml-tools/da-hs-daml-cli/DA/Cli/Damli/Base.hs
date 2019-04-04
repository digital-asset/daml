-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damli.Base
    ( module DA.Cli.Options
    , module DA.Cli.Output
    , module DA.Prelude
    , CommandM
    , Command)
where
import           DA.Cli.Options
import           DA.Cli.Output
import           DA.Prelude

type CommandM = IO
type Command  = IO ()

