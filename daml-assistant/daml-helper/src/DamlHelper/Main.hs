-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlHelper.Main (main) where

import Control.Exception
import Data.Foldable
import Options.Applicative.Extended
import System.Environment
import System.Exit
import System.IO

import DamlHelper.Run
import DamlHelper.Signals

main :: IO ()
main =
    withProgName "daml" $ go `catch` \(e :: DamlHelperError) -> do
        hPutStrLn stderr (displayException e)
        exitFailure
  where
    parserPrefs = prefs showHelpOnError
    go = do
         installSignalHandlers
         command <- customExecParser parserPrefs (info (commandParser <**> helper) idm)
         runCommand command

data Command
    = DamlStudio { replaceExtension :: ReplaceExtension, remainingArguments :: [String] }
    | RunJar { jarPath :: FilePath, remainingArguments :: [String] }
    | New { targetFolder :: FilePath, templateNameM :: Maybe String }
    | Migrate { targetFolder :: FilePath, pkgPathFrom :: FilePath, pkgPathTo :: FilePath }
    | Init { targetFolderM :: Maybe FilePath }
    | ListTemplates
    | Start { openBrowser :: OpenBrowser, startNavigator :: StartNavigator, onStartM :: Maybe String, waitForSignal :: WaitForSignal }

commandParser :: Parser Command
commandParser =
    subparser $ fold
         [ command "studio" (info (damlStudioCmd <**> helper) forwardOptions)
         , command "new" (info (newCmd <**> helper) idm)
         , command "migrate" (info (migrateCmd <**> helper) idm)
         , command "init" (info (initCmd <**> helper) idm)
         , command "start" (info (startCmd <**> helper) idm)
         , command "run-jar" (info runJarCmd forwardOptions)
         ]
    where damlStudioCmd = DamlStudio
              <$> option readReplacement
                  (long "replace" <>
                   help "Whether an existing extension should be overwritten. ('never', 'newer' or 'always' for bundled extension version, 'published' for official published version of extension, defaults to 'published')" <>
                   value ReplaceExtPublished
                  )
              <*> many (argument str (metavar "ARG"))
          runJarCmd = RunJar
              <$> argument str (metavar "JAR" <> help "Path to JAR relative to SDK path")
              <*> many (argument str (metavar "ARG"))
          newCmd = asum
              [ ListTemplates <$ flag' () (long "list" <> help "List the available project templates.")
              , New
                  <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located")
                  <*> optional (argument str (metavar "TEMPLATE" <> help ("Name of the template used to create the project (default: " <> defaultProjectTemplate <> ")")))
              ]
          migrateCmd =  Migrate
                  <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located")
                  <*> argument str (metavar "FROM_PATH" <> help "Path to the dar-package from which to migrate from")
                  <*> argument str (metavar "TO_PATH" <> help "Path to the dar-package to which to migrate to")
          initCmd = Init <$> optional (argument str (metavar "TARGET_PATH" <> help "Project folder to initialize."))
          startCmd = Start
                <$> (OpenBrowser <$> flagYesNoAuto "open-browser" True "Open the browser after navigator" idm)
                <*> (StartNavigator <$> flagYesNoAuto "start-navigator" True "Start navigator after sandbox" idm)
                <*> optional (option str (long "on-start" <> metavar "COMMAND" <> help "Command to run once sandbox and navigator are running."))
                <*> (WaitForSignal <$> flagYesNoAuto "wait-for-signal" True "Wait for Ctrl+C or interrupt after starting servers." idm)

          readReplacement :: ReadM ReplaceExtension
          readReplacement = maybeReader $ \case
              "never" -> Just ReplaceExtNever
              "newer" -> Just ReplaceExtNewer
              "always" -> Just ReplaceExtAlways
              "published" -> Just ReplaceExtPublished
              _ -> Nothing

runCommand :: Command -> IO ()
runCommand DamlStudio {..} = runDamlStudio replaceExtension remainingArguments
runCommand RunJar {..} = runJar jarPath remainingArguments
runCommand New {..} = runNew targetFolder templateNameM []
runCommand Migrate {..} = runMigrate targetFolder pkgPathFrom pkgPathTo
runCommand Init {..} = runInit targetFolderM
runCommand ListTemplates = runListTemplates
runCommand Start {..} = runStart startNavigator openBrowser onStartM waitForSignal
