-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Exception
import Control.Monad
import Control.Monad.Extra
import Data.Foldable
import Options.Applicative
import System.FilePath
import System.Directory.Extra
import System.Exit
import System.Process hiding (runCommand)
import System.IO
import System.IO.Error

import DAML.Project.Config

data Command
    = DamlStudio { overwriteExtension :: Bool, remainingArguments :: [String] }
    | RunJar { jarPath :: FilePath, remainingArguments :: [String] }
    | New { targetFolder :: FilePath, templateName :: String }
    | ListTemplates

commandParser :: Parser Command
commandParser =
    subparser $ foldMap
         (\(name, opts) -> command name (info (opts <**> helper) idm))
         [ ("studio", damlStudioCmd)
         , ("run-jar", runJarCmd)
         , ("new", newCmd)
         ]
    where damlStudioCmd = DamlStudio
              <$> switch (long "overwrite" <> help "Overwrite the VSCode extension if it already exists")
              <*> many (argument str (metavar "ARG"))
          runJarCmd = RunJar
              <$> argument str (metavar "JAR" <> help "Path to JAR relative to SDK path")
              <*> many (argument str (metavar "ARG"))
          newCmd = asum
              [ ListTemplates <$ flag' () (long "list" <> help "List the available project templates.")
              , New
                  <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located")
                  <*> argument str (metavar "TEMPLATE" <> help "Name of the template used to create the project (default: quickstart-java)" <> value "quickstart-java")
              ]

main :: IO ()
main = runCommand =<< execParser (info (commandParser <**> helper) idm)

runCommand :: Command -> IO ()
runCommand DamlStudio {..} = runDamlStudio overwriteExtension remainingArguments
runCommand RunJar {..} = runJar jarPath remainingArguments
runCommand New {..} = runNew targetFolder templateName
runCommand ListTemplates = runListTemplates

runDamlStudio :: Bool -> [String] -> IO ()
runDamlStudio overwriteExtension remainingArguments = do
    sdkPath <- getSdkPath
    vscodeExtensionsDir <- fmap (</> ".vscode/extensions") getHomeDirectory
    let vscodeExtensionName = "da-vscode-daml-extension"
    let vscodeExtensionSrcDir = sdkPath </> "studio"
    let vscodeExtensionTargetDir = vscodeExtensionsDir </> vscodeExtensionName
    when overwriteExtension $ removePathForcibly vscodeExtensionTargetDir
    installExtension vscodeExtensionSrcDir vscodeExtensionTargetDir
    exitCode <- withCreateProcess (proc "code" ("-w" : remainingArguments)) $ \_ _ _ -> waitForProcess
    exitWith exitCode

runJar :: FilePath -> [String] -> IO ()
runJar jarPath remainingArguments = do
    sdkPath <- getSdkPath
    let absJarPath = sdkPath </> jarPath
    exitCode <- withCreateProcess (proc "java" ("-jar" : absJarPath : remainingArguments)) $ \_ _ _ -> waitForProcess
    exitWith exitCode

getTemplatesFolder :: IO FilePath
getTemplatesFolder = fmap (</> "templates") getSdkPath

runNew :: FilePath -> String -> IO ()
runNew targetFolder templateName = do
    templatesFolder <- getTemplatesFolder
    let templateFolder = templatesFolder </> templateName
    unlessM (doesDirectoryExist templateFolder) $ do
        hPutStrLn stderr $ unlines
            [ "Template " <> show templateName <> " does not exist."
            , "Use `daml new --list` to see a list of available templates"
            ]
        exitFailure
    whenM (doesDirectoryExist targetFolder) $ do
        hPutStrLn stderr $ unlines
            [ "Directory " <> show targetFolder <> " already exists."
            , "Please specify a new directory for creating a project."
            ]
        exitFailure
    copyDirectory templateFolder targetFolder

runListTemplates :: IO ()
runListTemplates = do
    templatesFolder <- getTemplatesFolder
    templates <- listDirectory templatesFolder
    if null templates
       then putStrLn "No templates are available."
       else putStrLn $ unlines $
          "The following templates are available:" :
          map (\dir -> "  " <> takeFileName dir) templates

copyDirectory :: FilePath -> FilePath -> IO ()
copyDirectory src target = do
    files <- listFilesRecursive src
    forM_ files $ \file -> do
        let baseName = makeRelative src file
        let targetFile = target </> baseName
        createDirectoryIfMissing True (takeDirectory targetFile)
        copyFile file targetFile

installExtension :: FilePath -> FilePath -> IO ()
installExtension src target =
    catchJust
        (guard . isAlreadyExistsError)
        (createDirectoryLink src target)
        (-- We might want to emit a warning if the extension is for a different SDK version
         -- but medium term it probably makes more sense to add the extension to the marketplace
         -- and make it backwards compatible
         const $ pure ())
