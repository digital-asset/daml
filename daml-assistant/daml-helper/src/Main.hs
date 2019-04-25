-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main (main) where

import Data.Foldable
import Options.Applicative

import DamlHelper

main :: IO ()
main = runCommand =<< execParser (info (commandParser <**> helper) idm)

data Command
    = DamlStudio { overwriteExtension :: Bool, remainingArguments :: [String] }
    | RunJar { jarPath :: FilePath, remainingArguments :: [String] }
    | New { targetFolder :: FilePath, templateName :: String }
    | ListTemplates
    | Start

commandParser :: Parser Command
commandParser =
    subparser $ fold
         [ command "studio" (info (damlStudioCmd <**> helper) idm)
         , command "new" (info (newCmd <**> helper) idm)
         , command "start" (info (startCmd <**> helper) idm)
         , command "run-jar" (info runJarCmd forwardOptions)
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
          startCmd = pure Start

runCommand :: Command -> IO ()
runCommand DamlStudio {..} = runDamlStudio overwriteExtension remainingArguments
runCommand RunJar {..} = runJar jarPath remainingArguments
runCommand New {..} = runNew targetFolder templateName
runCommand ListTemplates = runListTemplates
runCommand Start = runStart

