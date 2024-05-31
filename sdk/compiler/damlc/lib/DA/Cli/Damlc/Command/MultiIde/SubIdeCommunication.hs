-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}

module DA.Cli.Damlc.Command.MultiIde.SubIdeCommunication (
  module DA.Cli.Damlc.Command.MultiIde.SubIdeCommunication
) where

import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar
import Control.Monad
import Control.Monad.STM
import qualified Data.Aeson as Aeson
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Types
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, mapMaybe)
import GHC.Conc (unsafeIOToSTM)
import qualified Language.LSP.Types as LSP
import System.Directory (doesFileExist)
import System.FilePath.Posix (takeDirectory)

-- Communication logic

-- Dangerous as does not hold the misSubIdesVar lock. If a shutdown is called whiled this is running, the message may not be sent.
unsafeSendSubIde :: SubIdeInstance -> LSP.FromClientMessage -> IO ()
unsafeSendSubIde ide = atomically . unsafeSendSubIdeSTM ide

unsafeSendSubIdeSTM :: SubIdeInstance -> LSP.FromClientMessage -> STM ()
unsafeSendSubIdeSTM ide = writeTChan (ideInHandleChannel ide) . Aeson.encode

sendAllSubIdes :: MultiIdeState -> LSP.FromClientMessage -> IO [PackageHome]
sendAllSubIdes miState msg = holdingIDEsAtomic miState $ \ides ->
  let ideInstances = mapMaybe ideDataMain $ Map.elems ides
   in forM ideInstances $ \ide -> ideHome ide <$ unsafeSendSubIdeSTM ide msg

sendAllSubIdes_ :: MultiIdeState -> LSP.FromClientMessage -> IO ()
sendAllSubIdes_ miState = void . sendAllSubIdes miState

getDirectoryIfFile :: FilePath -> IO FilePath
getDirectoryIfFile path = do
  isFile <- doesFileExist path
  pure $ if isFile then takeDirectory path else path

getSourceFileHome :: MultiIdeState -> FilePath -> STM PackageHome
getSourceFileHome miState path = do
  -- If the path is a file, we only care about the directory, as all files in the same directory share the same home
  dirPath <- unsafeIOToSTM $ getDirectoryIfFile path
  sourceFileHomes <- takeTMVar (misSourceFileHomesVar miState)
  case Map.lookup dirPath sourceFileHomes of
    Just home -> do
      putTMVar (misSourceFileHomesVar miState) sourceFileHomes
      unsafeIOToSTM $ logDebug miState $ "Found cached home for " <> path
      pure home
    Nothing -> do
      -- Safe as repeat prints are acceptable
      unsafeIOToSTM $ logDebug miState $ "No cached home for " <> path
      -- Read only operation, so safe within STM
      home <- unsafeIOToSTM $ fromMaybe (misDefaultPackagePath miState) <$> findHome dirPath
      unsafeIOToSTM $ logDebug miState $ "File system yielded " <> unPackageHome home
      putTMVar (misSourceFileHomesVar miState) $ Map.insert dirPath home sourceFileHomes
      pure home

sourceFileHomeHandleDamlFileDeleted :: MultiIdeState -> FilePath -> STM ()
sourceFileHomeHandleDamlFileDeleted miState path = do
  dirPath <- unsafeIOToSTM $ getDirectoryIfFile path
  modifyTMVar (misSourceFileHomesVar miState) $ Map.delete dirPath

-- When a daml.yaml changes, all files pointing to it are invalidated in the cache
sourceFileHomeHandleDamlYamlChanged :: MultiIdeState -> PackageHome -> STM ()
sourceFileHomeHandleDamlYamlChanged miState home = modifyTMVar (misSourceFileHomesVar miState) $ Map.filter (/=home)
