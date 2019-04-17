-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Base
    ( module DA.Cli.Options
    , module DA.Cli.Output
    , module DA.Prelude
    , CommandM
    , Command
    , getLogger
    )
where
import           DA.Cli.Options
import           DA.Cli.Output
import           DA.Prelude
import           DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified Data.Text as T
import qualified DA.Service.Logger                 as Logger
import qualified DA.Service.Logger.Impl.IO         as Logger.IO
import qualified DA.Service.Logger.Impl.Pure as Logger.Pure


type CommandM = IO
type Command  = IO ()


getLogger :: Compiler.Options -> T.Text -> IO (Logger.Handle IO)
getLogger Compiler.Options {optDebug} name =
    if optDebug
        then Logger.IO.newStderrLogger name
        else pure Logger.Pure.makeNopHandle
