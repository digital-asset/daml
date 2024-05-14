-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.Client (
  module DA.Cli.Damlc.Command.MultiIde.Client 
) where

import Control.Concurrent.STM.TChan
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import DA.Cli.Damlc.Command.MultiIde.Types
import qualified Language.LSP.Types as LSP

sendClientSTM :: MultiIdeState -> LSP.FromServerMessage -> STM ()
sendClientSTM miState = writeTChan (misToClientChan miState) . Aeson.encode

sendClient :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClient miState = atomically . sendClientSTM miState

-- Sends a message to the client, putting it at the start of the queue to be sent first
sendClientFirst :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClientFirst miState = atomically . unGetTChan (misToClientChan miState) . Aeson.encode
