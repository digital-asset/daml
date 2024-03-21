-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Concurrent
import Control.Concurrent.STM
import Control.Lens
import Control.Monad
import Control.Monad.Loops (untilM_)
import qualified Data.Aeson as Aeson
import Data.Aeson.Lens
import qualified Data.ByteString.Lazy as LBS
import Data.List.Extra
import Data.String (fromString)
import Data.Maybe (maybeToList, isJust)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as Vector
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket.Extended
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Daml.Helper.Util (waitForHttpServer, tokenFor, decodeCantonSandboxPort)
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent, callCommandSilentIn, subprocessEnv)
import DA.Test.Util
import DA.PortFile
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)

main :: IO ()
main = withSdkVersions $ do
    yarn : args <- getArgs
    withTempDir $ \tmpDir -> do
        oldPath <- getSearchPath
        javaPath <- locateRunfiles "local_jdk/bin"
        yarnPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> yarn)
        -- NOTE(Sofia): We don't use `script` on Windows.
        mbScriptPath <- if isWindows
            then pure Nothing
            else Just <$> locateRunfiles "script_nix/bin"
        limitJvmMemory defaultJvmMemoryLimits
        withArgs args (withEnv
            [ ("PATH", Just $ intercalate [searchPathSeparator] $ concat
                [ [javaPath, yarnPath]
                , maybeToList mbScriptPath
                , oldPath
                ])
            , ("TASTY_NUM_THREADS", Just "2")
            ] $ defaultMain (tests tmpDir))

hardcodedToken :: String -> T.Text
hardcodedToken alice = tokenFor [T.pack alice] "sandbox" "AssistantIntegrationTests"

authorizationHeaders :: String -> RequestHeaders
authorizationHeaders alice = [("Authorization", "Bearer " <> T.encodeUtf8 (hardcodedToken alice))]

withDamlServiceIn :: FilePath -> String -> [String] -> (ProcessHandle -> IO a) -> IO a
withDamlServiceIn path command args act = withDevNull $ \devNull -> do
    let proc' = (shell $ unwords $ ["daml", command, "--shutdown-stdin-close"] <> args)
          { std_out = UseHandle devNull
          , std_in = CreatePipe
          , cwd = Just path
          }
    withCreateProcess proc' $ \stdin _ _ ph -> do
        Just stdin <- pure stdin
        r <- act ph
        hClose stdin
        -- We tear things down gracefully instead of killing
        -- the process group so that waiting for the parent process
        -- ensures that all child processes are all dead too.
        -- Going via closing stdin works on Windows whereas tearing things
        -- down gracefully via SIGTERM isn’t as much of a thing so we use the former.
        _ <- waitForProcess ph
        pure r

data DamlStartResource = DamlStartResource
    { projDir :: FilePath
    , tmpDir :: FilePath
    , alice :: String
    , aliceHeaders :: RequestHeaders
    , startStdin :: Handle
    , stdoutChan :: TChan String
    , stop :: IO ()
    , sandboxPort :: PortNumber
    , jsonApiPort :: PortNumber
    }

damlStart :: SdkVersioned => FilePath -> IO DamlStartResource
damlStart tmpDir = do
    let projDir = tmpDir </> "assistant-integration-tests"
    createDirectoryIfMissing True (projDir </> "daml")
    let scriptOutputFile = "script-output.json"
    writeFileUTF8 (projDir </> "daml.yaml") $
        unlines
            [ "sdk-version: " <> sdkVersion
            , "name: assistant-integration-tests"
            , "version: \"1.0\""
            , "source: daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - daml-script"
            , "init-script: Main:init"
            , "script-options:"
            , "  - --output-file"
            , "  - " <> scriptOutputFile
            , "codegen:"
            , "  js:"
            , "    output-directory: ui/daml.js"
            , "    npm-scope: daml.js"
            , "  java:"
            , "    output-directory: ui/java"
            ]
    writeFileUTF8 (projDir </> "daml/Main.daml") $
        unlines
            [ "module Main where"
            , "import Daml.Script"
            , "template T with p : Party where signatory p"
            , "init : Script Party"
            , "init = do"
            , "  alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"Alice\")"
            , "  alice `submit` createCmd (T alice)"
            , "  pure alice"
            , "test : Int -> Script (Int, Int)"
            , "test x = pure (x, x + 1)"
            ]
    ports <- sandboxPorts
    jsonApiPort <- getFreePort
    env <- subprocessEnv []
    let startProc =
            (shell $ unwords
                [ "daml start"
                , "--start-navigator=no"
                , "--sandbox-port", show $ ledger ports
                , "--sandbox-admin-api-port", show $ admin ports
                , "--sandbox-domain-public-port", show $ domainPublic ports
                , "--sandbox-domain-admin-port", show $ domainAdmin ports
                , "--json-api-port", show jsonApiPort
                ]
            ) {std_in = CreatePipe, std_out = CreatePipe, cwd = Just projDir, create_group = True, env = Just env}
    (Just startStdin, Just startStdout, _, startPh) <- createProcess startProc
    outChan <- newBroadcastTChanIO
    outReader <- forkIO $ forever $ do
        line <- hGetLine startStdout
        atomically $ writeTChan outChan line
    waitForHttpServer 240 startPh
        (threadDelay 500000)
        ("http://localhost:" <> show jsonApiPort <> "/v1/query")
        (authorizationHeaders "Alice") -- dummy party here, not important
    scriptOutput <- readFileUTF8 (projDir </> scriptOutputFile)
    let alice = (read scriptOutput :: String)
    pure $
        DamlStartResource
            { projDir = projDir
            , tmpDir = tmpDir
            , sandboxPort = ledger ports
            , jsonApiPort = jsonApiPort
            , startStdin = startStdin
            , alice = alice
            , aliceHeaders = authorizationHeaders alice
            , stop = do
                interruptProcessGroupOf startPh
                killThread outReader
            , stdoutChan = outChan
            }

tests :: SdkVersioned => FilePath -> TestTree
tests tmpDir =
    withSdkResource $ \_ ->
        testGroup
            "Integration tests"
            [ testCase "daml version" $
                callCommandSilentIn tmpDir "daml version"
            , testCase "daml --help" $
                callCommandSilentIn tmpDir "daml --help"
            , testCase "daml new --list" $
                callCommandSilentIn tmpDir "daml new --list"
            , packagingTests tmpDir
            , damlToolTests
            , withResource (damlStart (tmpDir </> "sandbox-canton")) stop damlStartTests
            , damlStartNotSharedTest
            , cleanTests cleanDir
            , templateTests
            , codegenTests codegenDir
            , cantonTests
            ]
  where
    cleanDir = tmpDir </> "clean"
    codegenDir = tmpDir </> "codegen"

-- Most of the packaging tests are in the a separate test suite in
-- //compiler/damlc/tests:packaging. This only has a couple of
-- integration tests.
packagingTests :: SdkVersioned => FilePath -> TestTree
packagingTests tmpDir =
    testGroup
        "packaging"
        [ testCase "Build copy trigger" $ do
              let projDir = tmpDir </> "copy-trigger1"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
              callCommandSilentIn projDir "daml build"
              let dar = projDir </> ".daml" </> "dist" </> "copy-trigger1-0.0.1.dar"
              assertFileExists dar
        , testCase "Build copy trigger with LF version 1.dev" $ do
              let projDir = tmpDir </> "copy-trigger2"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
              callCommandSilentIn projDir "daml build --target 1.dev"
              let dar = projDir </> ".daml" </> "dist" </> "copy-trigger2-0.0.1.dar"
              assertFileExists dar
        , testCase "Build trigger with extra dependency" $ do
              let myDepDir = tmpDir </> "mydep"
              createDirectoryIfMissing True (myDepDir </> "daml")
              writeFileUTF8 (myDepDir </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: mydep"
                      , "version: \"1.0\""
                      , "source: daml"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      ]
              writeFileUTF8 (myDepDir </> "daml" </> "MyDep.daml") $ unlines ["module MyDep where"]
              callCommandSilentIn myDepDir "daml build -o mydep.dar"
              let myTriggerDir = tmpDir </> "mytrigger"
              createDirectoryIfMissing True (myTriggerDir </> "daml")
              writeFileUTF8 (myTriggerDir </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: mytrigger"
                      , "version: \"1.0\""
                      , "source: daml"
                      , "dependencies:"
                      , "  - daml-prim"
                      , "  - daml-stdlib"
                      , "  - daml-trigger"
                      , "  - " <> myDepDir </> "mydep.dar"
                      ]
              writeFileUTF8 (myTriggerDir </> "daml/Main.daml") $
                  unlines ["module Main where", "import MyDep ()", "import Daml.Trigger ()"]
              callCommandSilentIn myTriggerDir "daml build -o mytrigger.dar"
              let dar = myTriggerDir </> "mytrigger.dar"
              assertFileExists dar
        , testCase "Build Daml script example" $ do
              let projDir = tmpDir </> "script-example"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              callCommandSilentIn projDir "daml build"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar
        -- TODO: re-enable this test when the script-example template no longer specifies 1.15
        {- , testCase "Build Daml script example with LF version 1.dev" $ do
              let projDir = tmpDir </> "script-example1"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              callCommandSilentIn projDir "daml build --target 1.dev"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar -}
        , testCase "Package depending on daml-script and daml-trigger can use data-dependencies" $ do
              callCommandSilent $ unwords ["daml", "new", tmpDir </> "data-dependency"]
              callCommandSilentIn (tmpDir </> "data-dependency") "daml build -o data-dependency.dar"
              createDirectoryIfMissing True (tmpDir </> "proj")
              writeFileUTF8 (tmpDir </> "proj" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib, daml-script, daml-trigger]"
                      , "data-dependencies: [" <>
                        show (tmpDir </> "data-dependency" </> "data-dependency.dar") <>
                        "]"
                      ]
              writeFileUTF8 (tmpDir </> "proj" </> "A.daml") $
                  unlines
                      [ "module A where"
                      , "import Daml.Script"
                      , "import Main"
                      , "f = setup >> allocateParty \"foobar\""
          -- This also checks that we get the same Script type within an SDK version.
                      ]
              callCommandSilentIn (tmpDir </> "proj") "daml build"
        ]

-- Test tools that can run outside a daml project
damlToolTests :: TestTree
damlToolTests =
    testGroup
        "daml tools"
        [ testCase "OAuth 2.0 middleware startup" $ do
            withTempDir $ \tmpDir -> do
                middlewarePort <- getFreePort
                withDamlServiceIn tmpDir "oauth2-middleware"
                    [ "--address"
                    , "localhost"
                    , "--http-port"
                    , show middlewarePort
                    , "--oauth-auth"
                    , "http://localhost:0/authorize"
                    , "--oauth-token"
                    , "http://localhost:0/token"
                    , "--auth-jwt-hs256-unsafe"
                    , "jwt-secret"
                    , "--id"
                    , "client-id"
                    , "--secret"
                    , "client-secret"
                    ] $ \ ph -> do
                        let endpoint =
                                "http://localhost:" <> show middlewarePort <> "/livez"
                        waitForHttpServer 240 ph (threadDelay 500000) endpoint []
                        req <- parseRequest endpoint
                        manager <- newManager defaultManagerSettings
                        resp <- httpLbs req manager
                        responseBody resp @?= "{\"status\":\"pass\"}"
        ]

-- We are trying to run as many tests with the same `daml start` process as possible to safe time.
damlStartTests :: SdkVersioned => IO DamlStartResource -> TestTree
damlStartTests getDamlStart =
    -- We use testCaseSteps to make sure each of these tests runs in sequence, not in parallel.
    testCaseSteps "daml start" $ \step -> do
        let subtest :: forall t. String -> IO t -> IO t
            subtest m p = step m >> p
        subtest "sandbox and json-api come up" $ do
            DamlStartResource {jsonApiPort, alice, aliceHeaders} <- getDamlStart
            manager <- newManager defaultManagerSettings
            initialRequest <-
                parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/create"
            let createRequest =
                    initialRequest
                        { method = "POST"
                        , requestHeaders = aliceHeaders
                        , requestBody =
                            RequestBodyLBS $
                            Aeson.encode $
                            Aeson.object
                                [ "templateId" Aeson..= Aeson.String "Main:T"
                                , "payload" Aeson..= [alice]
                                ]
                        }
            createResponse <- httpLbs createRequest manager
            statusCode (responseStatus createResponse) @?= 200
        subtest "daml start invokes codegen" $ do
            DamlStartResource {projDir} <- getDamlStart
            didGenerateJsCode <- doesFileExist (projDir </> "ui" </> "daml.js" </> "assistant-integration-tests-1.0" </> "package.json")
            didGenerateJavaCode <- doesFileExist (projDir </> "ui" </> "java" </> "da" </> "internal" </> "template" </> "Archive.java")
            didGenerateJsCode @?= True
            didGenerateJavaCode @?= True
        subtest "run a daml ledger command" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            callCommandSilentIn projDir $ unwords
                ["daml", "ledger", "allocate-party", "--port", show sandboxPort, "Bob"]
        subtest "Run init-script" $ do
            DamlStartResource {jsonApiPort, aliceHeaders} <- getDamlStart
            initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
            let queryRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = aliceHeaders
                    , requestBody =
                        RequestBodyLBS $
                        Aeson.encode $
                        Aeson.object ["templateIds" Aeson..= [Aeson.String "Main:T"]]
                    }
            manager <- newManager defaultManagerSettings
            queryResponse <- httpLbs queryRequest manager
            statusCode (responseStatus queryResponse) @?= 200
            preview (key "result" . _Array . to Vector.length) (responseBody queryResponse) @?= Just 2
        subtest "Daml Script --input-file and --output-file" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            let dar = projDir </> ".daml" </> "dist" </> "assistant-integration-tests-1.0.dar"
            writeFileUTF8 (projDir </> "input.json") "0"
            callCommandSilentIn projDir $ unwords
                [ "daml script"
                , "--dar " <> dar <> " --script-name Main:test"
                , "--input-file input.json --output-file output.json"
                , "--ledger-host localhost --ledger-port " <> show sandboxPort
                ]
            contents <- readFileUTF8 (projDir </> "output.json")
            lines contents @?= ["{", "  \"_1\": 0,", "  \"_2\": 1", "}"]
        subtest "daml export script" $ do
            DamlStartResource {projDir, sandboxPort, alice} <- getDamlStart
            withTempDir $ \exportDir -> do
                callCommandSilentIn projDir $ unwords
                    [ "daml ledger export script"
                    , "--host localhost --port " <> show sandboxPort
                    , "--party", alice
                    , "--output " <> exportDir <> " --sdk-version " <> sdkVersion
                    ]
                didGenerateExportDaml <- doesFileExist (exportDir </> "Export.daml")
                didGenerateDamlYaml <- doesFileExist (exportDir </> "daml.yaml")
                didGenerateExportDaml @?= True
                didGenerateDamlYaml @?= True
        subtest "trigger service startup" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            triggerServicePort <- getFreePort
            withDamlServiceIn projDir "trigger-service"
                [ "--ledger-host"
                , "localhost"
                , "--ledger-port"
                , show sandboxPort
                , "--http-port"
                , show triggerServicePort
                , "--wall-clock-time"
                ] $ \ ph -> do
                    let endpoint = "http://localhost:" <> show triggerServicePort <> "/livez"
                    waitForHttpServer 240 ph (threadDelay 500000) endpoint []
                    req <- parseRequest endpoint
                    manager <- newManager defaultManagerSettings
                    resp <- httpLbs req manager
                    responseBody resp @?= "{\"status\":\"pass\"}"
        subtest "Navigator startup" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            navigatorPort :: Int <- fromIntegral <$> getFreePort
            -- This test just checks that navigator starts up and returns a 200 response.
            -- Nevertheless this would have caught a few issues on rules_nodejs upgrades
            -- where we got a 404 instead.
            withDamlServiceIn projDir "navigator"
                [ "server"
                , "localhost"
                , show sandboxPort
                , "--port"
                , show navigatorPort
                ] $ \ ph -> do
                    waitForHttpServer 240 ph
                        (threadDelay 500000)
                        ("http://localhost:" <> show navigatorPort)
                        []

        subtest "hot reload" $ do
            DamlStartResource {projDir, jsonApiPort, startStdin, stdoutChan, alice, aliceHeaders} <- getDamlStart
            stdoutReadChan <- atomically $ dupTChan stdoutChan
            writeFileUTF8 (projDir </> "daml/Main.daml") $
                unlines
                    [ "module Main where"
                    , "import Daml.Script"
                    , "template S with newFieldName : Party where signatory newFieldName"
                    , "init : Script Party"
                    , "init = do"
                    , "  let isAlice x = displayName x == Some \"Alice\""
                    , "  Some aliceDetails <- find isAlice <$> listKnownParties"
                    , "  let alice = party aliceDetails"
                    , "  alice `submit` createCmd (S alice)"
                    , "  pure alice"
                    ]
            hPutChar startStdin 'r'
            hFlush startStdin
            untilM_ (pure ()) $ do
                line <- atomically $ readTChan stdoutReadChan
                pure ("Rebuild complete" `isInfixOf` line)
            initialRequest <-
                parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
            manager <- newManager defaultManagerSettings
            let queryRequestT =
                    initialRequest
                        { method = "POST"
                        , requestHeaders = aliceHeaders
                        , requestBody =
                            RequestBodyLBS $
                            Aeson.encode $
                            Aeson.object ["templateIds" Aeson..= [Aeson.String "Main:T"]]
                        }
            let queryRequestS =
                    initialRequest
                        { method = "POST"
                        , requestHeaders = aliceHeaders
                        , requestBody =
                            RequestBodyLBS $
                            Aeson.encode $
                            Aeson.object ["templateIds" Aeson..= [Aeson.String "Main:S"]]
                        }
            queryResponseT <- httpLbs queryRequestT manager
            queryResponseS <- httpLbs queryRequestS manager
            -- check that there are no more active contracts of template T
            statusCode (responseStatus queryResponseT) @?= 200
            preview (key "result" . _Array) (responseBody queryResponseT) @?= Just Vector.empty
            -- check that a new contract of template S was created
            statusCode (responseStatus queryResponseS) @?= 200
            preview
                (key "result" . nth 0 . key "payload" . key "newFieldName")
                (responseBody queryResponseS) @?=
                Just (fromString alice)

        subtest "run a daml deploy without project parties" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            copyFile (projDir </> "daml.yaml") (projDir </> "daml.yaml.back")
            writeFileUTF8 (projDir </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: proj1"
                , "version: 0.0.1"
                , "source: daml"
                , "dependencies:"
                , "  - daml-prim"
                , "  - daml-stdlib"
                , "  - daml-script"
                ]
            callCommandSilentIn projDir $ unwords ["daml", "deploy", "--host localhost", "--port", show sandboxPort]
            copyFile (projDir </> "daml.yaml.back") (projDir </> "daml.yaml")

-- | daml start tests that don't use the shared server
damlStartNotSharedTest :: SdkVersioned => TestTree
damlStartNotSharedTest = testCase "daml start --sandbox-port=0" $
    withTempDir $ \tmpDir -> do
        writeFileUTF8 (tmpDir </> "daml.yaml") $
            unlines
                [ "sdk-version: " <> sdkVersion
                , "name: sandbox-options"
                , "version: \"1.0\""
                , "source: ."
                , "dependencies:"
                , "  - daml-prim"
                , "  - daml-stdlib"
                , "start-navigator: false"
                ]
        withDamlServiceIn tmpDir "start"
            [ "--sandbox-port=0"
            , "--json-api-port=0"
            , "--json-api-option=--port-file=jsonapi.port"
            ] $ \ ph -> do
                jsonApiPort <- readPortFile ph maxRetries (tmpDir </> "jsonapi.port")
                initialRequest <-
                    parseRequest $
                    "http://localhost:" <> show jsonApiPort <> "/v1/parties/allocate"
                let queryRequest =
                        initialRequest
                            { method = "POST"
                            , requestHeaders = authorizationHeaders "Alice"
                            , requestBody =
                                    RequestBodyLBS $
                                    Aeson.encode $
                                    Aeson.object ["identifierHint" Aeson..= ("Alice" :: String)]
                            }
                manager <- newManager defaultManagerSettings
                queryResponse <- httpLbs queryRequest manager
                let body = responseBody queryResponse
                assertBool ("result is unexpected: " <> show body) $
                    ("{\"result\":{\"identifier\":\"Alice::" `LBS.isPrefixOf` body) &&
                    ("\",\"isLocal\":true},\"status\":200}" `LBS.isSuffixOf` body)

-- | Ensure that daml clean removes precisely the files created by daml build.
cleanTests :: FilePath -> TestTree
cleanTests baseDir = testGroup "daml clean"
    [ cleanTestFor "skeleton"
    , cleanTestFor "quickstart-java"
    ]
    where
        cleanTestFor :: String -> TestTree
        cleanTestFor templateName =
            testCase ("daml clean test for " <> templateName <> " template") $ do
                createDirectoryIfMissing True baseDir
                let projectDir = baseDir </> ("proj-" <> templateName)
                callCommandSilentIn baseDir $ unwords ["daml", "new", projectDir, "--template", templateName]
                filesAtStart <- sort <$> listFilesRecursive projectDir
                callCommandSilentIn projectDir "daml build"
                callCommandSilentIn projectDir "daml clean"
                filesAtEnd <- sort <$> listFilesRecursive projectDir
                when (filesAtStart /= filesAtEnd) $ fail $ unlines
                    [ "daml clean did not remove all files produced by daml build."
                    , ""
                    , "    files at start:"
                    , unlines (map ("       "++) filesAtStart)
                    , "    files at end:"
                    , unlines (map ("       "++) filesAtEnd)
                    ]

templateTests :: TestTree
templateTests = testGroup "templates" $
    [ testCase name $ do
        withTempDir $ \tmpDir -> do
            let dir = tmpDir </> "foobar"
            callCommandSilentIn tmpDir $ unwords ["daml", "new", dir, "--template", name]
            callCommandSilentIn dir "daml build"
    | name <- templateNames
    ] <>
    [ testCase "quickstart-java, positional template" $ do
        withTempDir $ \tmpDir -> do
            let dir = tmpDir </> "foobar"
            -- Verify that the old syntax for `daml new` still works.
            callCommandSilentIn tmpDir $ unwords ["daml","new", dir, "quickstart-java"]
            contents <- readFileUTF8 $ dir </> "daml.yaml"
            assertInfixOf "name: quickstart" contents
    ]
  -- NOTE (MK) We might want to autogenerate this list at some point but for now
  -- this should be good enough.
  where templateNames =
            [ "copy-trigger"
            , "gsg-trigger"
            -- daml-intro-1 - daml-intro-6 are not full projects.
            , "daml-intro-7"
            , "daml-patterns"
            , "quickstart-java"
            , "script-example"
            , "skeleton"
            , "create-daml-app"
            ]

-- | Check we can generate language bindings.
codegenTests :: FilePath -> TestTree
codegenTests codegenDir = testGroup "daml codegen" (
    [ codegenTestFor "java" Nothing
    ] ++
    -- The '@daml/types' NPM package is not available on Windows which
    -- is required by 'daml2js'.
    [ codegenTestFor "js" Nothing | not isWindows ]
    )
    where
        codegenTestFor :: String -> Maybe String -> TestTree
        codegenTestFor lang namespace =
            testCase lang $ do
                createDirectoryIfMissing True codegenDir
                let projectDir = codegenDir </> ("proj-" ++ lang)
                callCommandSilentIn codegenDir $ unwords ["daml new", projectDir, "--template=skeleton"]
                callCommandSilentIn projectDir "daml build"
                let darFile = projectDir </> ".daml/dist/proj-" ++ lang ++ "-0.0.1.dar"
                    outDir  = projectDir </> "generated" </> lang
                when (lang == "js") $ do
                    let workspaces = Workspaces [makeRelative codegenDir outDir]
                    setupYarnEnv codegenDir workspaces [DamlTypes, DamlLedger]
                callCommandSilentIn projectDir $
                    unwords [ "daml", "codegen", lang
                            , darFile ++ maybe "" ("=" ++) namespace
                            , "-o", outDir]
                contents <- listDirectory (projectDir </> outDir)
                assertBool "bindings were written" (not $ null contents)

cantonTests :: TestTree
cantonTests = testGroup "daml sandbox"
    [ testCaseSteps "Can start Canton sandbox and run script" $ \step -> withTempDir $ \dir -> do
        step "Creating project"
        callCommandSilentIn dir $ unwords ["daml new", "skeleton", "--template=skeleton"]
        step "Building project"
        callCommandSilentIn (dir </> "skeleton") "daml build"
        step "Finding free ports"
        ledgerApiPort <- getFreePort
        adminApiPort <- getFreePort
        domainPublicApiPort <- getFreePort
        domainAdminApiPort <- getFreePort
        step "Staring Canton sandbox"
        let portFile = dir </> "canton-portfile.json"
        withDamlServiceIn (dir </> "skeleton") "sandbox"
            [ "--port", show ledgerApiPort
            , "--admin-api-port", show adminApiPort
            , "--domain-public-port", show domainPublicApiPort
            , "--domain-admin-port", show domainAdminApiPort
            , "--canton-port-file", portFile
            ] $ \ ph -> do
            -- wait for port file to be written
            _ <- readPortFileWith decodeCantonSandboxPort ph maxRetries portFile
            step "Uploading DAR"
            callCommandSilentIn (dir </> "skeleton") $ unwords
                ["daml ledger upload-dar --host=localhost --port=" <> show ledgerApiPort, ".daml/dist/skeleton-0.0.1.dar"]
            step "Running script"
            callCommandSilentIn (dir </> "skeleton") $ unwords
                [ "daml script"
                , "--dar", ".daml/dist/skeleton-0.0.1.dar"
                , "--script-name Main:setup"
                , "--ledger-host=localhost", "--ledger-port=" <> show ledgerApiPort
                ]
            step "Start canton-console"
            env <- getEnvironment
            let cmd = unwords
                    [ "daml canton-console"
                    , "--port", show ledgerApiPort
                    , "--admin-api-port", show adminApiPort
                    , "--domain-public-port", show domainPublicApiPort
                    , "--domain-admin-port", show domainAdminApiPort
                    ]
                -- NOTE (Sofia): We need to use `script` on Mac and Linux because of this Ammonite issue:
                --    https://github.com/com-lihaoyi/Ammonite/issues/276
                -- Also, script for Mac and script for Linux have incompatible CLIs for unfathomable reasons.
                -- Also, we need to set TERM to something, otherwise tput complains and crashes Ammonite.
                wrappedCmd
                    | isWindows = cmd
                    | isMac = "script -q -- tty.txt " <> cmd
                    | otherwise = concat ["script -q -c '", cmd, "'"]
                input =
                    [ "sandbox.health.running"
                    , "local.health.running"
                    , "exit" -- This "exit" is necessary on Linux, otherwise the REPL expects more input.
                             -- script on Linux doesn't transmit the EOF/^D to the REPL, unlike on Mac.
                    ]
                env' | isWindows || isJust (lookup "TERM" env) = Nothing
                     | otherwise = Just (("TERM", "xterm-16color") : env)
                proc' = (shell wrappedCmd) { cwd = Just dir, env = env' }
            output <- readCreateProcess proc' (unlines input)
            let outputLines = lines output
            -- NOTE (Sofia): We use `isInfixOf` extensively because
            --   the REPL output is full of color codes.
            Just res0 <- pure (find (isInfixOf "res0") outputLines)
            assertBool "sandbox participant is not running" ("true" `isInfixOf` res0)
            Just res1 <- pure (find (isInfixOf "res1") outputLines)
            assertBool "local domain is not running" ("true" `isInfixOf` res1)

    ]
