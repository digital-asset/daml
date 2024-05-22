-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.OpenFiles (
  addOpenFile,
  removeOpenFile,
  handleRemovedPackageOpenFiles,
  handleCreatedPackageOpenFiles,
  sendPackageDiagnostic,
) where

import Control.Monad
import Control.Monad.STM
import DA.Cli.Damlc.Command.MultiIde.ClientCommunication
import DA.Cli.Damlc.Command.MultiIde.SubIdeCommunication
import DA.Cli.Damlc.Command.MultiIde.Util
import DA.Cli.Damlc.Command.MultiIde.Types
import Data.Foldable (traverse_)
import Data.List (isPrefixOf)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Extended as TE
import GHC.Conc (unsafeIOToSTM)
import System.FilePath ((</>))

ideDiagnosticFiles :: SubIdeData -> [FilePath]
ideDiagnosticFiles ideData = (unPackageHome (ideDataHome ideData) </> "daml.yaml") : fmap unDamlFile (Set.toList $ ideDataOpenFiles ideData)

sendPackageDiagnostic :: MultiIdeState -> SubIdeData -> STM ()
sendPackageDiagnostic miState ideData@SubIdeData {ideDataDisabled = IdeDataDisabled {iddSeverity, iddMessage}} =
  traverse_ (sendClientSTM miState) $ fullFileDiagnostic iddSeverity (T.unpack iddMessage) <$> ideDiagnosticFiles ideData
sendPackageDiagnostic miState ideData =
  traverse_ (sendClientSTM miState) $ clearDiagnostics <$> ideDiagnosticFiles ideData

onOpenFiles :: MultiIdeState -> PackageHome -> (Set.Set DamlFile -> Set.Set DamlFile) -> STM ()
onOpenFiles miState home f = modifyTMVarM (misSubIdesVar miState) $ \ides -> do
  let ideData = lookupSubIde home ides
      ideData' = ideData {ideDataOpenFiles = f $ ideDataOpenFiles ideData}
  sendPackageDiagnostic miState ideData'
  pure $ Map.insert home ideData' ides

addOpenFile :: MultiIdeState -> PackageHome -> DamlFile -> STM ()
addOpenFile miState home file = do
  unsafeIOToSTM $ logInfo miState $ "Added open file " <> unDamlFile file <> " to " <> unPackageHome home
  onOpenFiles miState home $ Set.insert file

removeOpenFile :: MultiIdeState -> PackageHome -> DamlFile -> STM ()
removeOpenFile miState home file = do
  unsafeIOToSTM $ logInfo miState $ "Removed open file " <> unDamlFile file <> " from " <> unPackageHome home
  onOpenFiles miState home $ Set.delete file

-- Logic for moving open files between subIdes when packages are created/destroyed

-- When a daml.yaml is removed, its openFiles need to be distributed to another SubIde, else we'll forget that the client opened them
-- We handle this by finding a new home (using getSourceFileHome) for each open file of the current subIde, and assigning to that
-- We also send the open file notification to the new subIde(s) if they're already running
handleRemovedPackageOpenFiles :: MultiIdeState -> PackageHome -> IO ()
handleRemovedPackageOpenFiles miState home = withIDEsAtomic_ miState $ \ides -> do
  let ideData = lookupSubIde home ides
      moveOpenFile :: SubIdes -> DamlFile -> STM SubIdes
      moveOpenFile ides openFile = do
        -- getSourceFileHome caches on a directory level, so won't do that many filesystem operations
        newHome <- getSourceFileHome miState $ unDamlFile openFile
        let newHomeIdeData = lookupSubIde newHome ides
            newHomeIdeData' = newHomeIdeData {ideDataOpenFiles = Set.insert openFile $ ideDataOpenFiles newHomeIdeData}
        -- If we're moving the file to a disabled IDE, it should get the new warning
        sendPackageDiagnostic miState newHomeIdeData'
        forM_ (ideDataMain newHomeIdeData) $ \ide -> do
          -- Acceptable IO as read only operation
          content <- unsafeIOToSTM $ TE.readFileUtf8 $ unDamlFile openFile
          unsafeSendSubIdeSTM ide $ openFileNotification openFile content
        pure $ Map.insert newHome newHomeIdeData' ides
  ides' <- foldM moveOpenFile ides $ ideDataOpenFiles ideData
  pure $ Map.insert home (ideData {ideDataOpenFiles = mempty}) ides'

-- When a daml.yaml is created, we potentially move openFiles from other subIdes to this one
-- We do this by finding all SubIdes that sit above this package in the directory
--   (plus the default package, which is said to sit above all other packages)
-- We iterate their open files for any that sit below this package
--   i.e. this package sits between a daml file and its daml.yaml
-- We move these open files to the new package, sending Closed file messages to their former IDE
--   We also assume no subIde for this package existed already, as its daml.yaml was just created
--   so there is no need to send Open file messages to it
handleCreatedPackageOpenFiles :: MultiIdeState -> PackageHome -> IO ()
handleCreatedPackageOpenFiles miState home = withIDEsAtomic_ miState $ \ides -> do
  -- Iterate ides, return a list of open files, update ides and run monadically
  let shouldConsiderIde :: PackageHome -> Bool
      shouldConsiderIde oldHome = 
        oldHome == misDefaultPackagePath miState ||
          unPackageHome oldHome `isPrefixOf` unPackageHome home && oldHome /= home
      shouldMoveFile :: DamlFile -> Bool
      shouldMoveFile (DamlFile damlFilePath) = unPackageHome home `isPrefixOf` damlFilePath
      handleIde :: (SubIdes, Set.Set DamlFile) -> (PackageHome, SubIdeData) -> STM (SubIdes, Set.Set DamlFile)
      handleIde (ides, damlFiles) (oldHome, oldIdeData) | shouldConsiderIde oldHome = do
        let openFilesToMove = Set.filter shouldMoveFile $ ideDataOpenFiles oldIdeData
            updatedOldIdeData = oldIdeData {ideDataOpenFiles = ideDataOpenFiles oldIdeData Set.\\ openFilesToMove}
        forM_ (ideDataMain oldIdeData) $ \ide -> forM_ openFilesToMove $ unsafeSendSubIdeSTM ide . closeFileNotification
        pure (Map.insert oldHome updatedOldIdeData ides, openFilesToMove <> damlFiles)
      handleIde (ides, damlFiles) (oldHome, oldIdeData) =
        pure (Map.insert oldHome oldIdeData ides, damlFiles)
  (ides', movedFiles) <- foldM handleIde mempty $ Map.toList ides
  let ideData = lookupSubIde home ides
  -- Invalidate the home cache for every moved file
  traverse_ (sourceFileHomeHandleDamlFileDeleted miState . unDamlFile) movedFiles
  pure $ Map.insert home (ideData {ideDataOpenFiles = movedFiles <> ideDataOpenFiles ideData}) ides'
