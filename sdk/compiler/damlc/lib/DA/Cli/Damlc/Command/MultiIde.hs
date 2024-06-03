-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde (runMultiIde) where

import Control.Concurrent.Async (async, cancel, pollSTM)
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TVar
import Control.Exception(SomeException, fromException)
import Control.Monad
import Control.Monad.STM
import qualified Data.ByteString.Lazy.Char8 as BSLC
import DA.Cli.Damlc.Command.MultiIde.Handlers
import DA.Cli.Damlc.Command.MultiIde.PackageData
import DA.Cli.Damlc.Command.MultiIde.Parsing
import DA.Cli.Damlc.Command.MultiIde.SubIdeManagement
import DA.Cli.Damlc.Command.MultiIde.Types
import DA.Cli.Damlc.Command.MultiIde.Util
import qualified DA.Service.Logger as Logger
import Data.Either (lefts)
import Data.Foldable (traverse_)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, maybeToList)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import qualified SdkVersion.Class
import System.Directory (getCurrentDirectory)
import System.Exit (exitSuccess)
import System.FilePath.Posix ((</>))
import System.IO.Extra
import System.Process.Typed (ExitCode (..), getExitCodeSTM)

-- Main loop logic

createDefaultPackage :: SdkVersion.Class.SdkVersioned => IO (PackageHome, IO ())
createDefaultPackage = do
  (toPosixFilePath -> misDefaultPackagePath, cleanup) <- newTempDir
  writeFile (misDefaultPackagePath </> "daml.yaml") $ unlines
    [ "sdk-version: " <> SdkVersion.Class.sdkVersion
    , "name: daml-ide-default-environment"
    , "version: 1.0.0"
    , "source: ."
    , "dependencies:"
    , "  - daml-prim"
    , "  - daml-stdlib"
    ]
  pure (PackageHome misDefaultPackagePath, cleanup)

runMultiIde :: SdkVersion.Class.SdkVersioned => Logger.Priority -> [String] -> IO ()
runMultiIde loggingThreshold args = do
  homePath <- toPosixFilePath <$> getCurrentDirectory
  (misDefaultPackagePath, cleanupDefaultPackage) <- createDefaultPackage
  let misSubIdeArgs = if loggingThreshold <= Logger.Debug then "--debug" : args else args
  miState <- newMultiIdeState homePath misDefaultPackagePath loggingThreshold misSubIdeArgs subIdeMessageHandler unsafeAddNewSubIdeAndSend
  invalidPackageHomes <- updatePackageData miState

  -- Ensure we don't send messages to the client until it finishes initializing
  (onceUnblocked, unblock) <- makeIOBlocker

  logInfo miState $ "Running with logging threshold of " <> show loggingThreshold
  -- Client <- *****
  toClientThread <- async $ onceUnblocked $ forever $ do
    msg <- atomically $ readTChan $ misToClientChan miState
    logDebug miState $ "Pushing message to client:\n" <> BSLC.unpack msg 
    putChunk stdout msg

  -- Client -> Coord
  clientToCoordThread <- async $
    onChunks stdin $ clientMessageHandler miState unblock

  -- All invalid packages get spun up, so their errors are shown
  traverse_ (\home -> addNewSubIdeAndSend miState home Nothing) invalidPackageHomes

  let killAll :: IO ()
      killAll = do
        logDebug miState "Killing subIdes"
        holdingIDEs miState $ \ides -> foldM_ (unsafeShutdownIdeByHome miState) ides (Map.keys ides)
        logInfo miState "MultiIde shutdown"

      -- Get all outcomes from a SubIdeInstance (process and async failures/completions)
      subIdeInstanceOutcomes :: PackageHome -> SubIdeInstance -> STM [(PackageHome, SubIdeInstance, Either ExitCode SomeException)]
      subIdeInstanceOutcomes home ide = do
        mExitCode <- getExitCodeSTM (ideProcess ide)
        errs <- lefts . catMaybes <$> traverse pollSTM [ideInhandleAsync ide, ideOutHandleAsync ide, ideErrTextAsync ide]
        let mExitOutcome = (home, ide, ) . Left <$> mExitCode
            errorOutcomes = (home, ide, ) . Right <$> errs
        pure $ errorOutcomes <> maybeToList mExitOutcome

      -- Function folded over outcomes to update SubIdes, keep error list and list subIdes to reboot
      handleOutcome
        :: ([(PackageHome, SomeException)], SubIdes, [PackageHome])
        -> (PackageHome, SubIdeInstance, Either ExitCode SomeException)
        -> IO ([(PackageHome, SomeException)], SubIdes, [PackageHome])
      handleOutcome (errs, subIdes, toRestart) (home, ide, outcomeType) =
        case outcomeType of
          -- subIde process exits
          Left exitCode -> do
            logDebug miState $ "SubIde at " <> unPackageHome home <> " exited, cleaning up."
            traverse_ hTryClose [ideInHandle ide, ideOutHandle ide, ideErrHandle ide]
            traverse_ cancel [ideInhandleAsync ide, ideOutHandleAsync ide, ideErrTextAsync ide]
            stderrContent <- readTVarIO (ideErrText ide)
            currentTime <- getCurrentTime
            let ideData = lookupSubIde home subIdes
                isMainIde = ideDataMain ideData == Just ide
                isCrash = exitCode /= ExitSuccess
                ideData' = ideData
                  { ideDataClosing = Set.delete ide $ ideDataClosing ideData
                  , ideDataMain = if isMainIde then Nothing else ideDataMain ideData
                  , ideDataFailures = 
                      if isCrash && isMainIde
                        then take 2 $ (currentTime, stderrContent) : ideDataFailures ideData
                        else ideDataFailures ideData
                  }
                toRestart' = if isCrash && isMainIde then home : toRestart else toRestart
            when (isCrash && isMainIde) $
              logWarning miState $ "Proccess failed, stderr content:\n" <> T.unpack stderrContent
              
            pure (errs, Map.insert home ideData' subIdes, toRestart')
          -- handler thread errors
          Right exception -> pure ((home, exception) : errs, subIdes, toRestart)

  forever $ do
    (outcomes, clientThreadExceptions) <- atomically $ do
      subIdes <- readTMVar $ misSubIdesVar miState
      
      outcomes <- fmap concat $ forM (Map.toList subIdes) $ \(home, subIdeData) -> do
        mainSubIdeOutcomes <- maybe (pure []) (subIdeInstanceOutcomes home) $ ideDataMain subIdeData
        closingSubIdesOutcomes <- concat <$> traverse (subIdeInstanceOutcomes home) (Set.toList $ ideDataClosing subIdeData)
        pure $ mainSubIdeOutcomes <> closingSubIdesOutcomes
      
      clientThreadExceptions <- lefts . catMaybes <$> traverse pollSTM [toClientThread, clientToCoordThread]
      
      when (null outcomes && null clientThreadExceptions) retry

      pure (outcomes, clientThreadExceptions)

    unless (null clientThreadExceptions) $
      if any (\e -> fromException @ExitCode e == Just ExitSuccess) clientThreadExceptions
        then do
          logWarning miState "Exiting!"
          cleanupDefaultPackage
          exitSuccess
        else error $ "1 or more client thread handlers failed: " <> show clientThreadExceptions
    
    unless (null outcomes) $ do
      errs <- withIDEs miState $ \ides -> do
        (errs, ides', idesToRestart) <- foldM handleOutcome ([], ides, []) outcomes
        ides'' <- foldM (\ides home -> unsafeAddNewSubIdeAndSend miState ides home Nothing) ides' idesToRestart
        pure (ides'', errs)

      when (not $ null errs) $ do
        cleanupDefaultPackage
        killAll
        error $ "SubIde handlers failed with following errors:\n" <> unlines ((\(home, err) -> unPackageHome home <> " => " <> show err) <$> errs)
