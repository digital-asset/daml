-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ApplicativeDo #-}
module DA.Daml.Helper.Main (main) where

import Control.Exception.Safe
import Control.Monad
import DA.Bazel.Runfiles
import Data.Foldable
import Data.List.Extra
import Numeric.Natural
import Options.Applicative.Extended
import System.Environment
import System.Exit
import System.IO
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

main :: IO ()
main = do
    -- Save the runfiles environment to work around
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
    | RunPlatformJar
        { args :: [String]
        , logbackConfig :: FilePath
        , shutdownStdinClose :: Bool
        }
    | New { targetFolder :: FilePath, appTemplate :: AppTemplate }
    | CreateDamlApp { targetFolder :: FilePath }
    -- ^ CreateDamlApp is sufficiently special that in addition to
    -- `daml new foobar create-daml-app` we also make `daml create-daml-app foobar` work.
    | Init { targetFolderM :: Maybe FilePath }
    | ListTemplates
    | Start
      { sandboxPortM :: Maybe SandboxPortSpec
      , openBrowser :: OpenBrowser
      , startNavigator :: Maybe StartNavigator
      , navigatorPort :: NavigatorPort
      , jsonApiCfg :: JsonApiConfig
      , onStartM :: Maybe String
      , waitForSignal :: WaitForSignal
      , sandboxOptions :: SandboxOptions
      , navigatorOptions :: NavigatorOptions
      , jsonApiOptions :: JsonApiOptions
      , scriptOptions :: ScriptOptions
      , shutdownStdinClose :: Bool
      , sandboxClassic :: SandboxClassic
      }
    | Deploy { flags :: LedgerFlags }
    | LedgerListParties { flags :: LedgerFlags, json :: JsonFlag }
    | LedgerAllocateParties { flags :: LedgerFlags, parties :: [String] }
    | LedgerUploadDar { flags :: LedgerFlags, darPathM :: Maybe FilePath }
    | LedgerFetchDar { flags :: LedgerFlags, pid :: String, saveAs :: FilePath }
    | LedgerReset {flags :: LedgerFlags}
    | LedgerExport { flags :: LedgerFlags, remainingArguments :: [String] }
    | LedgerNavigator { flags :: LedgerFlags, remainingArguments :: [String] }
    | Codegen { lang :: Lang, remainingArguments :: [String] }
    | PackagesList {flags :: LedgerFlags}
    | CantonSandbox
      { ports :: CantonPorts
      , remainingArguments :: [String]
      }

data AppTemplate
  = AppTemplateDefault
  | AppTemplateViaOption String
  | AppTemplateViaArgument String

newtype ShowJsonApi = ShowJsonApi {_showJsonApi :: Bool}

commandParser :: Parser Command
commandParser = subparser $ fold
    [ command "studio" (info (damlStudioCmd <**> helper) forwardOptions)
    , command "new" (info (newCmd <**> helper) idm)
    , command "create-daml-app" (info (createDamlAppCmd <**> helper) idm)
    , command "init" (info (initCmd <**> helper) idm)
    , command "start" (info (startCmd <**> helper) idm)
    , command "deploy" (info (deployCmd <**> helper) deployCmdInfo)
    , command "ledger" (info (ledgerCmd <**> helper) ledgerCmdInfo)
    , command "run-jar" (info runJarCmd forwardOptions)
    , command "run-platform-jar" (info runPlatformJarCmd forwardOptions)
    , command "codegen" (info (codegenCmd <**> helper) forwardOptions)
    , command "packages" (info (packagesCmd <**> helper) packagesCmdInfo)
    , command "canton-sandbox" (info (cantonSandboxCmd <**> helper) cantonSandboxCmdInfo)
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

    runPlatformJarCmd = RunPlatformJar
        <$> many (argument str (metavar "ARG"))
        <*> strOption (long "logback-config")
        <*> stdinCloseOpt

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

    createDamlAppCmd =
        CreateDamlApp <$>
        argument str (metavar "TARGET_PATH" <> help "Path where the new project should be located")

    initCmd = Init
        <$> optional (argument str (metavar "TARGET_PATH" <> help "Project folder to initialize."))

    startCmd = Start
        <$> optional (option (maybeReader (toSandboxPortSpec <=< readMaybe)) (long "sandbox-port" <> metavar "PORT_NUM" <> help "Port number for the sandbox"))
        <*> (OpenBrowser <$> flagYesNoAuto "open-browser" True "Open the browser after navigator" idm)
        <*> optional navigatorFlag
        <*> navigatorPortOption
        <*> jsonApiCfg
        <*> optional (option str (long "on-start" <> metavar "COMMAND" <> help "Command to run once sandbox and navigator are running."))
        <*> (WaitForSignal <$> flagYesNoAuto "wait-for-signal" True "Wait for Ctrl+C or interrupt after starting servers." idm)
        <*> (SandboxOptions <$> many (strOption (long "sandbox-option" <> metavar "SANDBOX_OPTION" <> help "Pass option to sandbox")))
        <*> (NavigatorOptions <$> many (strOption (long "navigator-option" <> metavar "NAVIGATOR_OPTION" <> help "Pass option to navigator")))
        <*> (JsonApiOptions <$> many (strOption (long "json-api-option" <> metavar "JSON_API_OPTION" <> help "Pass option to HTTP JSON API")))
        <*> (ScriptOptions <$> many (strOption (long "script-option" <> metavar "SCRIPT_OPTION" <> help "Pass option to Daml script interpreter")))
        <*> stdinCloseOpt
        <*> (SandboxClassic <$> switch (long "sandbox-classic" <> help "Deprecated. Run with Sandbox Classic."))

    navigatorFlag =
        -- We do not use flagYesNoAuto here since that doesn’t allow us to differentiate
        -- if the flag was passed explicitly or not.
        StartNavigator <$>
        option reader (long "start-navigator" <> help helpText <> completeWith ["true", "false"] <> idm)
        where
            reader = eitherReader $ \case
                -- We allow for both yes and true since we want a boolean in daml.yaml
                "true" -> Right True
                "yes" -> Right True
                "false" -> Right False
                "no" -> Right False
                "auto" -> Right True
                s -> Left ("Expected \"yes\", \"true\", \"no\", \"false\" or \"auto\" but got " <> show s)
            -- To make things less confusing, we do not mention yes, no and auto here.
            helpText = "Start navigator as part of daml start. Can be set to true or false. Defaults to true."

    navigatorPortOption = NavigatorPort <$> option auto
        (long "navigator-port"
        <> metavar "PORT_NUM"
        <> value 7500
        <> help "Port number for navigator (default is 7500).")

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
        <$> ledgerFlags (ShowJsonApi False)

    jsonApiCfg = JsonApiConfig <$> option
        readJsonApiPort
        ( long "json-api-port"
       <> value (Just $ JsonApiPort 7575)
       <> help "Port that the HTTP JSON API should listen on or 'none' to disable it"
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
            ,  command "scala" $ info codegenScalaCmd (progDesc "(deprecated)" <> forwardOptions)
            ,  command "js" $ info codegenJavaScriptCmd forwardOptions
            ]
        ]

    codegenJavaCmd = Codegen Java <$> many (argument str (metavar "ARG"))
    codegenScalaCmd = Codegen Scala <$> many (argument str (metavar "ARG"))
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
                (progDesc "Allocate parties on ledger")
            , command "upload-dar" $ info
                (ledgerUploadDarCmd <**> helper)
                (progDesc "Upload DAR file to ledger")
            , command "fetch-dar" $ info
                (ledgerFetchDarCmd <**> helper)
                (progDesc "Fetch DAR from ledger into file")
            , command "navigator" $ info
                (ledgerNavigatorCmd <**> helper)
                (forwardOptions <> progDesc "Launch Navigator on ledger")
            ]
        , subparser $ internal <> fold -- hidden subcommands
            [ command "allocate-party" $ info
                (ledgerAllocatePartyCmd <**> helper)
                (progDesc "Allocate a single party on ledger")
            , command "reset" $ info
                (ledgerResetCmd <**> helper)
                (progDesc "Archive all currently active contracts.")
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
        <$> ledgerFlags (ShowJsonApi True)
        <*> fmap JsonFlag (switch $ long "json" <> help "Output party list in JSON")

    packagesListCmd = PackagesList
        <$> ledgerFlags (ShowJsonApi True)

    ledgerAllocatePartiesCmd = LedgerAllocateParties
        <$> ledgerFlags (ShowJsonApi True)
        <*> many (argument str (metavar "PARTY" <> help "Parties to be allocated on the ledger (defaults to project parties if empty)"))

    -- same as allocate-parties but requires a single party.
    ledgerAllocatePartyCmd = LedgerAllocateParties
        <$> ledgerFlags (ShowJsonApi True)
        <*> fmap (:[]) (argument str (metavar "PARTY" <> help "Party to be allocated on the ledger"))

    ledgerUploadDarCmd = LedgerUploadDar
        <$> ledgerFlags (ShowJsonApi True)
        <*> optional (argument str (metavar "PATH" <> help "DAR file to upload (defaults to project DAR)"))

    ledgerFetchDarCmd = LedgerFetchDar
        <$> ledgerFlags (ShowJsonApi True)
        <*> option str (long "main-package-id" <> metavar "PKGID" <> help "Fetch DAR for this package identifier.")
        <*> option str (short 'o' <> long "output" <> metavar "PATH" <> help "Save fetched DAR into this file.")

    ledgerResetCmd = LedgerReset
        <$> ledgerFlags (ShowJsonApi True)

    ledgerExportCmd = subparser $
        command "script" (info scriptOptions (progDesc "Export ledger state in Daml script format" <> forwardOptions))
      where
        scriptOptions = LedgerExport
          <$> ledgerFlags (ShowJsonApi False)
          <*> (("script":) <$> many (argument str (metavar "ARG" <> help "Arguments forwarded to export.")))

    ledgerNavigatorCmd = LedgerNavigator
        <$> ledgerFlags (ShowJsonApi False)
        <*> many (argument str (metavar "ARG" <> help "Extra arguments to navigator."))

    ledgerFlags showJsonApi = LedgerFlags
        <$> httpJsonFlag showJsonApi
        <*> sslConfig
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

    httpJsonFlag :: ShowJsonApi -> Parser LedgerApi
    httpJsonFlag (ShowJsonApi showJsonApi)
      | showJsonApi =
        flag Grpc HttpJson $
        long "json-api" <> help "Use the HTTP JSON API instead of gRPC"
      | otherwise = pure Grpc

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

    cantonSandboxCmd = CantonSandbox
      <$> (CantonPorts
             <$> option auto (long "port" <> value 6865)
             <*> option auto (long "admin-api-port" <> value 6866)
             <*> option auto (long "domain-public-port" <> value 6867)
             <*> option auto (long "domain-admin-port" <> value 6868)
          )
      <*> many (argument str (metavar "ARG"))

    cantonSandboxCmdInfo =
        forwardOptions

runCommand :: Command -> IO ()
runCommand = \case
    DamlStudio {..} -> runDamlStudio replaceExtension remainingArguments
    RunJar {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $
        runJar jarPath mbLogbackConfig remainingArguments
    RunPlatformJar {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $
        runPlatformJar args logbackConfig
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
    CreateDamlApp{..} -> runNew targetFolder (Just "create-daml-app")
    Init {..} -> runInit targetFolderM
    ListTemplates -> runListTemplates
    Start {..} ->
        (if shutdownStdinClose then withCloseOnStdin else id) $
        runStart
            sandboxPortM
            startNavigator
            navigatorPort
            jsonApiCfg
            openBrowser
            onStartM
            waitForSignal
            sandboxOptions
            navigatorOptions
            jsonApiOptions
            scriptOptions
            sandboxClassic
    Deploy {..} -> runDeploy flags
    LedgerListParties {..} -> runLedgerListParties flags json
    PackagesList {..} -> runLedgerListPackages0 flags
    LedgerAllocateParties {..} -> runLedgerAllocateParties flags parties
    LedgerUploadDar {..} -> runLedgerUploadDar flags darPathM
    LedgerFetchDar {..} -> runLedgerFetchDar flags pid saveAs
    LedgerReset {..} -> runLedgerReset flags
    LedgerExport {..} -> runLedgerExport flags remainingArguments
    LedgerNavigator {..} -> runLedgerNavigator flags remainingArguments
    Codegen {..} -> runCodegen lang remainingArguments
    CantonSandbox {..} -> runCantonSandbox ports remainingArguments
