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
    | Sandbox { port :: SandboxPort, remainingArguments :: [String] }
    | Start { darPath :: FilePath }

commandParser :: Parser Command
commandParser =
    subparser $ foldMap
         (\(name, opts) -> command name (info (opts <**> helper) idm))
         [ ("studio", damlStudioCmd)
         , ("run-jar", runJarCmd)
         , ("new", newCmd)
         , ("sandbox", sandboxCmd)
         , ("start", startCmd)
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
          sandboxCmd = Sandbox
              <$> option sandboxPortReader (long "port" <> help "Port used by the sandbox")
              <*> many (argument str (metavar "ARG"))
          startCmd = Start <$> argument str (metavar "DAR_PATH" <> help "Path to DAR that should be loaded")

runCommand :: Command -> IO ()
runCommand DamlStudio {..} = runDamlStudio overwriteExtension remainingArguments
runCommand RunJar {..} = runJar jarPath remainingArguments
runCommand New {..} = runNew targetFolder templateName
runCommand ListTemplates = runListTemplates
runCommand Sandbox {..} = runSandbox port remainingArguments
runCommand Start {..} = runStart darPath

sandboxPortReader :: ReadM SandboxPort
sandboxPortReader = SandboxPort <$> auto
