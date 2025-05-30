-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ApplicativeDo #-}
module DA.Daml.Helper.Main (main) where

import Control.Exception.Safe
import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.Foldable
import Data.List.Extra
import Numeric.Natural
import Options.Applicative.Extended
import Options.Applicative
import System.Environment
import System.Exit
import System.IO.Extra
import System.Process (showCommandForUser)
import Text.Read (readMaybe)
import DA.Signals
import DA.Daml.Helper.Init
import DA.Daml.Helper.Ledger
import DA.Daml.Helper.New
import DA.Daml.Helper.Start
import DA.Daml.Helper.Studio
import DA.Daml.Helper.Util
import DA.Daml.Helper.Codegen

import SdkVersion (withSdkVersions)

main :: IO ()
main = do
    -- Save the runfiles environment to work around
    forM_ [stdout, stderr] $ \h -> hSetBuffering h LineBuffering
    -- https://gitlab.haskell.org/ghc/ghc/-/issues/18418.
    setRunfilesEnv
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
    | RunJar
        { jarPath :: FilePath
        , mbLogbackConfig :: Maybe FilePath
        -- ^ Both file paths are relative to the SDK directory.
        , remainingArguments :: [String]
        , shutdownStdinClose :: Bool
        }
    | New { targetFolder :: FilePath, appTemplate :: AppTemplate }
    | Init { targetFolderM :: Maybe FilePath }
    | ListTemplates
    | Start
        { startOptions :: StartOptions
        , shutdownStdinClose :: Bool
        }
    | Deploy { flags :: LedgerFlags }
    | LedgerListParties { flags :: LedgerFlags, json :: JsonFlag }
    | LedgerAllocateParties { flags :: LedgerFlags, parties :: [String] }
    | LedgerUploadDar { flags :: LedgerFlags, dryRun :: DryRun, darPathM :: Maybe FilePath }
    | LedgerFetchDar { flags :: LedgerFlags, pid :: String, saveAs :: FilePath }
    -- | LedgerReset {flags :: LedgerFlags}
    | LedgerExport { flags :: LedgerFlags, remainingArguments :: [String] }
    | Codegen { lang :: Lang, remainingArguments :: [String] }
    | PackagesList {flags :: LedgerFlags}
    | CantonSandbox
        { cantonOptions :: CantonOptions
        , portFileM :: Maybe FilePath
        , darPaths :: [FilePath]
        , remainingArguments :: [String]
        , shutdownStdinClose :: Bool
        }
    | CantonRepl
        { cantonReplOptions :: CantonReplOptions
        , remainingArguments :: [String]
        }
    | UpgradeCheck
        { darPaths :: [String]
        , upgradeCheckTool :: UpgradeCheckTool
        }

data UpgradeCheckTool = UCTParticipant | UCTCompiler | UCTBoth
  deriving (Show, Eq, Ord)

data AppTemplate
  = AppTemplateDefault
  | AppTemplateViaOption String
  | AppTemplateViaArgument String

commandParser :: Parser Command
commandParser = subparser $ fold
    [ command "studio" (info (damlStudioCmd <**> helper) forwardOptions)
    , command "new" (info (newCmd <**> helper) idm)
    , command "init" (info (initCmd <**> helper) idm)
    , command "start" (info (startCmd <**> helper) idm)
    , command "deploy" (info (deployCmd <**> helper) deployCmdInfo)
    , command "ledger" (info (ledgerCmd <**> helper) ledgerCmdInfo)
    , command "run-jar" (info runJarCmd forwardOptions)
    , command "codegen" (info (codegenCmd <**> helper) forwardOptions)
    , command "packages" (info (packagesCmd <**> helper) packagesCmdInfo)
    , command "sandbox" (info (cantonSandboxCmd <**> helper) cantonSandboxCmdInfo)
    , command "canton-console" (info (cantonReplCmd <**> helper) cantonReplCmdInfo)
    , command "upgrade-check" (info upgradeCheckCmd forwardOptions)
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

     -- We need to push this into the commands since it will end up after the command due to the way daml-assistant
    -- invokes daml-helper.
    stdinCloseOpt = switch (hidden <> long "shutdown-stdin-close" <> help "Shut down when stdin is closed, disabled by default")

    runJarCmd = RunJar
        <$> argument str (metavar "JAR" <> help "Path to JAR relative to SDK path")
        <*> optional (strOption (long "logback-config"))
        <*> many (argument str (metavar "ARG"))
        <*> stdinCloseOpt

    upgradeCheckCmd = UpgradeCheck
        <$> many (argument str (metavar "ARG"))
        <*> (flag' UCTParticipant (long "participant") <|> flag' UCTCompiler (long "compiler") <|> flag' UCTBoth (long "both"))

    newCmd =
        let templateHelpStr = "Name of the template used to create the project (default: " <> defaultProjectTemplate <> ")"
            appTemplateFlag = asum
              [ AppTemplateViaOption
                  <$> strOption (long "template" <> metavar "TEMPLATE" <> help templateHelpStr)
              , AppTemplateViaArgument <$> argument str (metavar "TEMPLATE_ARG" <> help ("Deprecated. " <> templateHelpStr))
              , pure AppTemplateDefault
              ]
        in asum
        [ ListTemplates <$ flag' () (long "list" <> help "List the available project templates.")
        , New
            <$> argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located")
            <*> appTemplateFlag
        ]

    initCmd = Init
        <$> optional (argument str (metavar "TARGET_PATH" <> help "Project folder to initialize."))

    startCmd = do
        sandboxPortM <- sandboxPortOpt "sandbox-port" "Port number for the sandbox"
        jsonApiPortM <- jsonApiPortOpt "json-api-port" "Port that the HTTP JSON API should listen on or 'none' to disable it"
        onStartM <- optional (option str (long "on-start" <> metavar "COMMAND" <> help "Command to run once sandbox is running."))
        shouldWaitForSignal <- flagYesNoAuto "wait-for-signal" True "Wait for Ctrl+C or interrupt after starting servers." idm
        sandboxOptions <- many (strOption (long "sandbox-option" <> metavar "SANDBOX_OPTION" <> help "Pass option to sandbox"))
        scriptOptions <- many (strOption (long "script-option" <> metavar "SCRIPT_OPTION" <> help "Pass option to Daml script interpreter"))
        shutdownStdinClose <- stdinCloseOpt
        sandboxPortSpec <- sandboxCantonPortSpecOpt
        pure $ Start StartOptions{..} shutdownStdinClose

    sandboxPortOpt name desc =
        optional (option (maybeReader (toSandboxPortSpec <=< readMaybe))
            (long name <> metavar "PORT_NUM" <> help desc))

    sandboxCantonPortSpecOpt = do
        adminApiSpec <- sandboxPortOpt "sandbox-admin-api-port" "Port number for the canton admin API (--sandbox-canton only)"
        sequencerPublicApiSpec <- sandboxPortOpt "sandbox-sequencer-public-port" "Port number for the canton sequencer public API (--sandbox-canton only)"
        sequencerAdminApiSpec <- sandboxPortOpt "sandbox-sequencer-admin-port" "Port number for the canton sequencer admin API (--sandbox-canton only)"
        mediatorAdminApiSpec <- sandboxPortOpt "sandbox-mediator-admin-port" "Port number for the canton mediator admin API (--sandbox-canton only)"
        pure SandboxCantonPortSpec {..}

    deployCmdInfo = mconcat
        [ progDesc $ concat
              [ "Deploy the current Daml project to a remote Daml ledger. "
              , "This will allocate the project's parties on the ledger "
              , "(if missing) and upload the project's built DAR file. You "
              , "can specify the ledger in daml.yaml with the ledger.host and "
              , "ledger.port options, or you can pass the --host and --port "
              , "flags to this command instead. "
              , "If the ledger is authenticated, you should pass "
              , "the name of the file containing the token "
              , "using the --access-token-file flag."
              ]
        , deployFooter
        ]

    deployFooter = footer "See https://docs.daml.com/deploy/ for more information on deployment."

    deployCmd = Deploy
        <$> ledgerFlags

    jsonApiPortOpt name desc = option
        readJsonApiPort
        ( long name
       <> value (Just $ JsonApiPort 7575)
       <> help desc
        )

    readJsonApiPort = eitherReader $ \case
        "none" -> Right Nothing
        s -> maybe
            (Left $ "Failed to parse port " <> show s)
            (Right . Just . JsonApiPort)
            (readMaybe s)

    codegenCmd = asum
        [ subparser $ fold
            [  command "java" $ info codegenJavaCmd forwardOptions
            ,  command "js" $ info codegenJavaScriptCmd forwardOptions
            ]
        ]

    codegenJavaCmd = Codegen Java <$> many (argument str (metavar "ARG"))
    codegenJavaScriptCmd = Codegen JavaScript <$> many (argument str (metavar "ARG"))

    ledgerCmdInfo = mconcat
        [ forwardOptions
        , progDesc $ concat
              [ "Interact with a remote Daml ledger. You can specify "
              , "the ledger in daml.yaml with the ledger.host and "
              , "ledger.port options, or you can pass the --host "
              , "and --port flags to each command below. "
              , "If the ledger is authenticated, you should pass "
              , "the name of the file containing the token "
              , "using the --access-token-file flag or the `daml.access-token-file` "
              , "field in daml.yaml."
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
                (progDesc "Allocate parties on ledger if they don't exist")
            , command "upload-dar" $ info
                (ledgerUploadDarCmd <**> helper)
                (progDesc "Upload DAR file to ledger")
            , command "fetch-dar" $ info
                (ledgerFetchDarCmd <**> helper)
                (progDesc "Fetch DAR from ledger into file")
            ]
        , subparser $ internal <> fold -- hidden subcommands
            [ command "allocate-party" $ info
                (ledgerAllocatePartyCmd <**> helper)
                (progDesc "Allocate a single party on ledger if it doesn't exist")
            -- , command "reset" $ info
            --     (ledgerResetCmd <**> helper)
            --     (progDesc "Archive all currently active contracts.")
            , command "export" $ info
                (ledgerExportCmd <**> helper)
                (forwardOptions <> progDesc "Export ledger state.")
            ]
        ]

    packagesCmd =
        subparser $
        command "list" $
        info (packagesListCmd <**> helper) (progDesc "List deployed dalf packages on ledger")

    packagesCmdInfo =
        mconcat
            [ forwardOptions
            , progDesc $
              concat
                  [ "Query packages of a remote Daml ledger. "
                  , "You can specify the ledger in daml.yaml with the "
                  , "ledger.host and ledger.port options, or you can pass "
                  , "the --host and --port flags to each command below. "
                  , "If the ledger is authenticated, you should pass "
                  , "the name of the file containing the token "
                  , "with the ledger.access-token-file option in daml.yaml or the "
                  , "--access-token-file flag."
                  ]
            ]

    ledgerListPartiesCmd = LedgerListParties
        <$> ledgerFlags
        <*> fmap JsonFlag (switch $ long "json" <> help "Output party list in JSON")

    packagesListCmd = PackagesList
        <$> ledgerFlags

    ledgerAllocatePartiesCmd = LedgerAllocateParties
        <$> ledgerFlags
        <*> many (argument str (metavar "PARTY" <> help "Parties to be allocated on the ledger if they don't exist (defaults to project parties if empty)"))

    -- same as allocate-parties but requires a single party.
    ledgerAllocatePartyCmd = LedgerAllocateParties
        <$> ledgerFlags
        <*> fmap (:[]) (argument str (metavar "PARTY" <> help "Party to be allocated on the ledger if it doesn't exist"))

    ledgerUploadDarCmd =
        LedgerUploadDar
            <$> ledgerFlags
            <*> (DryRun <$> switch (long "dry-run" <> help "Only check the DAR's validity according to the ledger, do not actually upload it."))
            <*> optional (argument str (metavar "PATH" <> help "DAR file to upload (defaults to project DAR)"))
    
    ledgerFetchDarCmd = LedgerFetchDar
        <$> ledgerFlags
        <*> option str (long "main-package-id" <> metavar "PKGID" <> help "Fetch DAR for this package identifier.")
        <*> option str (short 'o' <> long "output" <> metavar "PATH" <> help "Save fetched DAR into this file.")

    -- ledgerResetCmd = LedgerReset
    --     <$> ledgerFlags

    ledgerExportCmd = subparser $
        command "script" (info scriptOptions (progDesc "Export ledger state in Daml script format" <> forwardOptions))
      where
        scriptOptions = LedgerExport
          <$> ledgerFlags
          <*> (("script":) <$> many (argument str (metavar "ARG" <> help "Arguments forwarded to export.")))

    ledgerFlags = LedgerFlags
        <$> sslConfig
        <*> timeoutOption
        <*> hostFlag
        <*> portFlag
        <*> accessTokenFileFlag
        <*> maxInboundSizeFlag

    sslConfig :: Parser (Maybe ClientSSLConfig)
    sslConfig = do
        tls <- switch $ mconcat
            [ long "tls"
            , help "Enable TLS for the connection to the ledger. This is implied if --cacrt, --pem or --crt are passed"
            ]
        mbCACert <- optional $ strOption $ mconcat
            [ long "cacrt"
            , help "The crt file to be used as the trusted root CA."
            ]
        mbClientKeyCertPair <- optional $ liftA2 ClientSSLKeyCertPair
            (strOption $ mconcat
                 [ long "pem"
                 , help "The pem file to be used as the private key in mutual authentication."
                 ]
            )
            (strOption $ mconcat
                 [ long "crt"
                 , help "The crt file to be used as the cert chain in mutual authentication."
                 ]
            )
        return $ case (tls, mbCACert, mbClientKeyCertPair) of
            (False, Nothing, Nothing) -> Nothing
            (_, _, _) -> Just ClientSSLConfig
                { serverRootCert = mbCACert
                , clientSSLKeyCertPair = mbClientKeyCertPair
                , clientMetadataPlugin = Nothing
                }

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

    accessTokenFileFlag :: Parser (Maybe FilePath)
    accessTokenFileFlag = optional . option str $
        long "access-token-file"
        <> metavar "TOKEN_PATH"
        <> help "Path to the token-file for ledger authorization"

    maxInboundSizeFlag :: Parser (Maybe Natural)
    maxInboundSizeFlag = optional . option auto $
        long "max-inbound-message-size"
        <> metavar "INT"
        <> help "Optional max inbound message size in bytes. This only affect connections via gRPC."

    timeoutOption :: Parser TimeoutSeconds
    timeoutOption = option auto $ mconcat
        [ long "timeout"
        , metavar "INT"
        , value 60
        , help "Timeout of gRPC operations in seconds. Defaults to 60s. Must be > 0."
        ]

    cantonHelpSwitch =
      switch $
      long "canton-help" <>
      help "Display the help of the underlying Canton JAR instead of the Sandbox wrapper. This is only required for advanced options."

    -- These options are common enough that we want them to show up in --help instead of only in
    -- --canton-help.
    cantonConfigOpts =
      many $
      option str $
      long "config" <>
      short 'c' <>
      metavar "FILE" <>
      help (unwords [ "Set configuration file(s)."
                    , "If several configuration files assign values to the same key, the last value is taken."
                    ])

    cantonSandboxCmd = do
        cantonOptions <- do
            cantonLedgerApi <- option auto (long "port" <> value (ledger defaultSandboxPorts))
            cantonAdminApi <- option auto (long "admin-api-port" <> value (admin defaultSandboxPorts))
            cantonSequencerPublicApi <- option auto (long "sequencer-public-port" <> value (sequencerPublic defaultSandboxPorts))
            cantonSequencerAdminApi <- option auto (long "sequencer-admin-port" <> value (sequencerAdmin defaultSandboxPorts))
            cantonMediatorAdminApi <- option auto (long "mediator-admin-port" <> value (mediatorAdmin defaultSandboxPorts))
            cantonJsonApi <- optional $ option auto (long "json-api-port"
                <> help "Port that the HTTP JSON API should listen on, omit to disable it")
            cantonJsonApiPortFileM <- optional $ option str (long "json-api-port-file" <> metavar "PATH"
                <> help "File to write canton json-api port when ready")
            cantonPortFileM <- optional $ option str (long "canton-port-file" <> metavar "PATH"
                <> help "File to write canton participant ports when ready")
            cantonStaticTime <- StaticTime <$>
                (flag' True (long "static-time") <|>
                 flag' False (long "wall-clock-time") <|>
                 pure False)
            cantonHelp <- cantonHelpSwitch
            cantonConfigFiles <- cantonConfigOpts
            pure CantonOptions{..}
        portFileM <- optional $ option str (long "port-file" <> metavar "PATH"
            <> help "File to write ledger API port when ready")
        darPaths <- many $ option str (long "dar" <> metavar "PATH"
            <> help "DAR file to upload to sandbox")
        remainingArguments <- many (argument str (metavar "ARG"))
        shutdownStdinClose <- stdinCloseOpt
        pure CantonSandbox {..}

    cantonSandboxCmdInfo =
        forwardOptions

    cantonReplOpt = do
        host <- option str (long "host" <> value "127.0.0.1")
        ledgerApi <- option auto (long "port" <> value (ledger defaultSandboxPorts))
        adminApi <- option auto (long "admin-api-port" <> value (admin defaultSandboxPorts))
        sequencerPublicApi <- option auto (long "domain-public-port" <> value (sequencerPublic defaultSandboxPorts))
        sequencerAdminApi <- option auto (long "domain-admin-port" <> value (sequencerAdmin defaultSandboxPorts))
        pure $ CantonReplOptions
            [ CantonReplParticipant
                { crpName = "sandbox"
                , crpLedgerApi = Just (CantonReplApi host ledgerApi)
                , crpAdminApi = Just (CantonReplApi host adminApi)
                }
            ]
            [ CantonReplDomain
                { crdName = "local"
                , crdPublicApi = Just (CantonReplApi host sequencerPublicApi)
                , crdAdminApi = Just (CantonReplApi host sequencerAdminApi)
                }
            ]

    cantonReplCmd = do
        cantonReplOptions <- cantonReplOpt
        remainingArguments <- many (argument str (metavar "ARG"))
        pure CantonRepl {..}

    cantonReplCmdInfo =
        forwardOptions

runCommand :: Command -> IO ()
runCommand = \case
    DamlStudio {..} -> runDamlStudio replaceExtension remainingArguments
    RunJar {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $
        runJar jarPath mbLogbackConfig remainingArguments
    New {..} -> do
        templateNameM <- case appTemplate of
            AppTemplateDefault -> pure Nothing
            AppTemplateViaOption templateName -> pure (Just templateName)
            AppTemplateViaArgument templateName -> do
                hPutStrLn stderr $ unlines
                    [ "WARNING: Specifying the template via"
                    , ""
                    , "    " <> showCommandForUser "daml" ["new", targetFolder, templateName]
                    , ""
                    , "is deprecated. The recommended way of specifying the template is"
                    , ""
                    , "    " <> showCommandForUser "daml" ["new", targetFolder, "--template", templateName]
                    ]
                pure (Just templateName)
        runNew targetFolder templateNameM
    Init {..} -> runInit targetFolderM
    ListTemplates -> runListTemplates
    Start {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $
        runStart startOptions
    Deploy {..} -> runDeploy flags
    LedgerListParties {..} -> runLedgerListParties flags json
    PackagesList {..} -> runLedgerListPackages0 flags
    LedgerAllocateParties {..} -> runLedgerAllocateParties flags parties
    LedgerUploadDar {..} -> runLedgerUploadDar flags dryRun darPathM
    LedgerFetchDar {..} -> withSdkVersions $ runLedgerFetchDar flags pid saveAs
    -- LedgerReset {..} -> runLedgerReset flags
    LedgerExport {..} -> runLedgerExport flags remainingArguments
    Codegen {..} -> runCodegen lang remainingArguments
    CantonSandbox {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $ do
            putStrLn "Starting Canton sandbox."
            withCantonSandbox cantonOptions remainingArguments $ \(_, sandboxPort) -> do
                putStrLn ("Listening at port " <> show sandboxPort)
                forM_ darPaths $ \darPath -> do
                    runLedgerUploadDar (sandboxLedgerFlags sandboxPort) (DryRun False) (Just darPath)
                whenJust portFileM $ \portFile -> do
                    putStrLn ("Writing ledger API port to " <> portFile)
                    writeFileUTF8 portFile (show sandboxPort)
                putStrLn "Canton sandbox is ready."
    CantonRepl {..} ->
        runCantonRepl cantonReplOptions remainingArguments
    UpgradeCheck {..} -> do
        when (upgradeCheckTool `elem` [UCTParticipant, UCTBoth]) $
          runJar "daml-sdk/daml-sdk.jar" (Just "daml-sdk/script-logback.xml") ("upgrade-check": darPaths)
        when (upgradeCheckTool `elem` [UCTCompiler, UCTBoth]) $
          runDamlc ("upgrade-check" : darPaths)
