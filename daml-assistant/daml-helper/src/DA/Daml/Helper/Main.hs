-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Main (main) where

import Control.Exception
import Data.Foldable
import Data.List.Extra
import Options.Applicative.Extended
import System.Environment
import System.Exit
import System.IO
import Text.Read (readMaybe)

import DA.Signals
import DA.Daml.Helper.Run

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
    | Start
      { sandboxPortM :: Maybe SandboxPort
      , openBrowser :: OpenBrowser
      , startNavigator :: StartNavigator
      , jsonApiCfg :: JsonApiConfig
      , onStartM :: Maybe String
      , waitForSignal :: WaitForSignal
      }
    | Deploy { flags :: HostAndPortFlags }
    | LedgerListParties { flags :: HostAndPortFlags, json :: JsonFlag }
    | LedgerAllocateParties { flags :: HostAndPortFlags, parties :: [String] }
    | LedgerUploadDar { flags :: HostAndPortFlags, darPathM :: Maybe FilePath }
    | LedgerNavigator { flags :: HostAndPortFlags, remainingArguments :: [String] }

commandParser :: Parser Command
commandParser = subparser $ fold
    [ command "studio" (info (damlStudioCmd <**> helper) forwardOptions)
    , command "new" (info (newCmd <**> helper) idm)
    , command "migrate" (info (migrateCmd <**> helper) idm)
    , command "init" (info (initCmd <**> helper) idm)
    , command "start" (info (startCmd <**> helper) idm)
    , command "deploy" (info (deployCmd <**> helper) deployCmdInfo)
    , command "ledger" (info (ledgerCmd <**> helper) ledgerCmdInfo)
    , command "run-jar" (info runJarCmd forwardOptions)
    ]
  where

    damlStudioCmd = DamlStudio
        <$> option readReplacement
            (long "replace" <>
                help "Whether an existing extension should be overwritten. ('never' or 'always' for bundled extension version, 'published' for official published version of extension, defaults to 'published')" <>
                value ReplaceExtPublished
                )
        <*> many (argument str (metavar "ARG"))

    readReplacement = maybeReader $ \arg ->
        case lower arg of
            "never" -> Just ReplaceExtNever
            "always" -> Just ReplaceExtAlways
            "published" -> Just ReplaceExtPublished
            _ -> Nothing

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
        <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be   located")
        <*> argument str (metavar "FROM_PATH" <> help "Path to the dar-package from which to migrate from")
        <*> argument str (metavar "TO_PATH" <> help "Path to the dar-package to which to migrate to")

    initCmd = Init
        <$> optional (argument str (metavar "TARGET_PATH" <> help "Project folder to initialize."))

    startCmd = Start
        <$> optional (SandboxPort <$> option auto (long "sandbox-port" <> metavar "PORT_NUM" <>     help "Port number for the sandbox"))
        <*> (OpenBrowser <$> flagYesNoAuto "open-browser" True "Open the browser after navigator" idm)
        <*> (StartNavigator <$> flagYesNoAuto "start-navigator" True "Start navigator after sandbox" idm)
        <*> jsonApiCfg
        <*> optional (option str (long "on-start" <> metavar "COMMAND" <> help "Command to run once sandbox and navigator are running."))
        <*> (WaitForSignal <$> flagYesNoAuto "wait-for-signal" True "Wait for Ctrl+C or interrupt after starting servers." idm)

    deployCmdInfo = mconcat
        [ progDesc $ concat
              [ "Deploy the current DAML project to a remote DAML ledger. "
              , "This will allocate the project's parties on the ledger "
              , "(if missing) and upload the project's built DAR file. You "
              , "can specify the ledger in daml.yaml with the ledger.host and "
              , "ledger.port options, or you can pass the --host and --port "
              , "flags to this command instead."
              ]
        , deployFooter
        ]

    deployFooter = footer "See https://docs.daml.com/deploy/ for more information on deployment."

    deployCmd = Deploy
        <$> hostAndPortFlags

    jsonApiCfg = JsonApiConfig <$> option
        readJsonApiPort
        ( long "json-api-port"
       <> value Nothing -- Disabled by default until https://github.com/digital-asset/daml/issues/2788 is resolved.
       <> help "Port that the HTTP JSON API should listen on or 'none' to disable it"
        )

    readJsonApiPort = eitherReader $ \case
        "none" -> Right Nothing
        s -> maybe
            (Left $ "Failed to parse port " <> show s)
            (Right . Just . JsonApiPort)
            (readMaybe s)

    ledgerCmdInfo = mconcat
        [ forwardOptions
        , progDesc $ concat
              [ "Interact with a remote DAML ledger. You can specify "
              , "the ledger in daml.yaml with the ledger.host and "
              , "ledger.port options, or you can pass the --host "
              , "and --port flags to each command below."
              ]
        , deployFooter
        ]

    ledgerCmd = asum
        [ subparser $ fold
            [ command "list-parties" $ info
                (ledgerListPartiesCmd <**> helper)
                (progDesc "List parties known to ledger")
            , command "allocate-parties" $ info
                (ledgerAllocatePartiesCmd <**> helper)
                (progDesc "Allocate parties on ledger")
            , command "upload-dar" $ info
                (ledgerUploadDarCmd <**> helper)
                (progDesc "Upload DAR file to ledger")
            , command "navigator" $ info
                (ledgerNavigatorCmd <**> helper)
                (forwardOptions <> progDesc "Launch Navigator on ledger")
            ]
        , subparser $ internal <> fold -- hidden subcommands
            [ command "allocate-party" $ info
                (ledgerAllocatePartyCmd <**> helper)
                (progDesc "Allocate a single party on ledger")
            ]
        ]

    ledgerListPartiesCmd = LedgerListParties
        <$> hostAndPortFlags
        <*> fmap JsonFlag (switch $ long "json" <> help "Output party list in JSON")

    ledgerAllocatePartiesCmd = LedgerAllocateParties
        <$> hostAndPortFlags
        <*> many (argument str (metavar "PARTY" <> help "Parties to be allocated on the ledger (defaults to project parties if empty)"))

    -- same as allocate-parties but requires a single party.
    ledgerAllocatePartyCmd = LedgerAllocateParties
        <$> hostAndPortFlags
        <*> fmap (:[]) (argument str (metavar "PARTY" <> help "Party to be allocated on the ledger"))

    ledgerUploadDarCmd = LedgerUploadDar
        <$> hostAndPortFlags
        <*> optional (argument str (metavar "PATH" <> help "DAR file to upload (defaults to project DAR)"))

    ledgerNavigatorCmd = LedgerNavigator
        <$> hostAndPortFlags
        <*> many (argument str (metavar "ARG" <> help "Extra arguments to navigator."))

    hostAndPortFlags = HostAndPortFlags
        <$> hostFlag
        <*> portFlag

    hostFlag :: Parser (Maybe String)
    hostFlag = optional . option str $
        long "host"
        <> metavar "HOST_NAME"
        <> help "Hostname for the ledger"

    portFlag :: Parser (Maybe Int)
    portFlag = optional . option auto $
        long "port"
        <> metavar "PORT_NUM"
        <> help "Port number for the ledger"

runCommand :: Command -> IO ()
runCommand DamlStudio {..} = runDamlStudio replaceExtension remainingArguments
runCommand RunJar {..} = runJar jarPath remainingArguments
runCommand New {..} = runNew targetFolder templateNameM []
runCommand Migrate {..} = runMigrate targetFolder pkgPathFrom pkgPathTo
runCommand Init {..} = runInit targetFolderM
runCommand ListTemplates = runListTemplates
runCommand Start {..} = runStart sandboxPortM startNavigator jsonApiCfg openBrowser onStartM waitForSignal
runCommand Deploy {..} = runDeploy flags
runCommand LedgerListParties {..} = runLedgerListParties flags json
runCommand LedgerAllocateParties {..} = runLedgerAllocateParties flags parties
runCommand LedgerUploadDar {..} = runLedgerUploadDar flags darPathM
runCommand LedgerNavigator {..} = runLedgerNavigator flags remainingArguments
