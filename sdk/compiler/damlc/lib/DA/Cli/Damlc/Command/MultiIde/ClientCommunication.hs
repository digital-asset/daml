-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.ClientCommunication (
  module DA.Cli.Damlc.Command.MultiIde.ClientCommunication 
) where

import Control.Concurrent.STM.TChan
import Control.Concurrent.MVar
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import Data.Maybe
import qualified Data.Set as Set
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.Util
import qualified Language.LSP.Types as LSP
import System.FilePath

sendClientSTM :: MultiIdeState -> LSP.FromServerMessage -> STM ()
sendClientSTM miState = writeTChan (misToClientChan miState) . Aeson.encode

sendClient :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClient miState = atomically . sendClientSTM miState

-- Sends a message to the client, putting it at the start of the queue to be sent first
sendClientFirst :: MultiIdeState -> LSP.FromServerMessage -> IO ()
sendClientFirst miState = atomically . unGetTChan (misToClientChan miState) . Aeson.encode

reportGlobalErrorChange :: MultiIdeState -> (GlobalErrors -> GlobalErrors) -> IO [PackageHome]
reportGlobalErrorChange miState f = modifyMVar (misGlobalErrors miState) $ \ge ->
  let newGe = f ge
      hasErrors ge' = isJust (geUpdatePackageError ge') || isJust (geResolutionError ge')
      multiPackagePath = misMultiPackageHome miState </> "multi-package.yaml"
   in case (hasErrors ge, hasErrors newGe) of
      -- No longer erroring
      (True, False) -> do
        logDebug miState "Recovered from global errors"
        sendClient miState $ clearDiagnostics multiPackagePath
        pure (newGe {gePendingHomes = Set.empty}, Set.toList $ gePendingHomes newGe)
      -- Nothing to be done
      (False, False) -> pure (newGe, [])
      -- Errors to be reported
      _ -> do
        logDebug miState $ "New global errors: " <> show newGe
        sendClient miState $ fullFileDiagnostic LSP.DsError (show newGe) multiPackagePath
        pure (newGe, [])

-- Returns if the package was added, thus should be disabled
addPackageHomeIfErrored :: MultiIdeState -> PackageHome -> IO Bool
addPackageHomeIfErrored miState home = do
  modifyMVar (misGlobalErrors miState) $ \ge -> pure $
    if isJust (geUpdatePackageError ge) || isJust (geResolutionError ge)
      then (ge { gePendingHomes = home `Set.insert` gePendingHomes ge}, True)
      else (ge, False)

reportUpdatePackageError :: MultiIdeState -> Maybe String -> IO [PackageHome]
reportUpdatePackageError miState err = reportGlobalErrorChange miState (\ge -> ge {geUpdatePackageError = err})

reportResolutionError :: MultiIdeState -> Maybe String -> IO [PackageHome]
reportResolutionError miState err = reportGlobalErrorChange miState (\ge -> ge {geResolutionError = err})
