-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Base
    ( module DA.Cli.Options
    , module DA.Daml.Compiler.Output
    , getLogger
    )
where
import           DA.Cli.Options
import           DA.Daml.Compiler.Output
import DA.Daml.Options.Types
import qualified Data.Text as T
import qualified DA.Service.Logger                 as Logger
import qualified DA.Service.Logger.Impl.IO         as Logger.IO

getLogger :: Options -> T.Text -> IO (Logger.Handle IO)
getLogger Options {optLogLevel} name = Logger.IO.newStderrLogger optLogLevel name
