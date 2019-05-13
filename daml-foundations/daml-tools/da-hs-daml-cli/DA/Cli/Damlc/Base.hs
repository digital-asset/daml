-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Base
    ( module DA.Cli.Options
    , module DA.Cli.Output
    , CommandM
    , Command
    , getLogger
    )
where
import           DA.Cli.Options
import           DA.Cli.Output
import           DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified Data.Text as T
import qualified DA.Service.Logger                 as Logger
import qualified DA.Service.Logger.Impl.IO         as Logger.IO


type CommandM = IO
type Command  = IO ()


getLogger :: Compiler.Options -> T.Text -> IO (Logger.Handle IO)
getLogger Compiler.Options {optDebug} name =
    if optDebug
        then Logger.IO.newStderrLogger Logger.Debug name
        else Logger.IO.newStderrLogger Logger.Warning name
