-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTests (main) where

import Conduit hiding (connect)
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception.Extra
import Control.Lens
import Control.Monad
import qualified Data.Aeson as Aeson
import Data.Aeson.Lens
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.HashMap.Strict as HashMap
import Data.List.Extra
import qualified Data.Map as Map
import Data.Maybe (maybeToList)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Typeable (Typeable)
import qualified Data.Vector as Vector
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import qualified Web.JWT as JWT

import DA.Bazel.Runfiles
import DA.Daml.Assistant.FreePort (getFreePort, socketHints)
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Daml.Helper.Util (waitForConnectionOnPort, waitForHttpServer)
-- import DA.PortFile
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent)
import DA.Test.Util
import DA.PortFile
import SdkVersion

main :: IO ()
main = do
    yarn : args <- getArgs
    withTempDir $ \tmpDir -> do
    oldPath <- getSearchPath
    javaPath <- locateRunfiles "local_jdk/bin"
    mvnPath <- locateRunfiles "mvn_dev_env/bin"
    tarPath <- locateRunfiles "tar_dev_env/bin"
    yarnPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> yarn)
    -- NOTE: `COMSPEC` env. variable on Windows points to cmd.exe, which is required to be present
    -- on the PATH as mvn.cmd executes cmd.exe
    mbComSpec <- getEnv "COMSPEC"
    let mbCmdDir = takeDirectory <$> mbComSpec
    limitJvmMemory defaultJvmMemoryLimits
    withArgs args (withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] $ (tarPath : javaPath : mvnPath : yarnPath : oldPath) ++ maybeToList mbCmdDir)
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMain (tests tmpDir))

hardcodedToken :: T.Text
hardcodedToken =
    JWT.encodeSigned
        (JWT.HMACSecret "secret")
        mempty
        mempty
            { JWT.unregisteredClaims =
                  JWT.ClaimsMap $
                  Map.fromList
                      [ ( "https://daml.com/ledger-api"
                        , Aeson.Object $
                          HashMap.fromList
                              [ ("actAs", Aeson.toJSON ["Alice" :: T.Text])
                              , ("ledgerId", "MyLedger")
                              , ("applicationId", "foobar")
                              ])
                      ]
            }

authorizationHeaders :: RequestHeaders
authorizationHeaders = [("Authorization", "Bearer " <> T.encodeUtf8 hardcodedToken)]

data DamlStartResource = DamlStartResource
    { projDir :: FilePath
    , tmpDir :: FilePath
    , startStdin :: Maybe Handle
    , startPh :: ProcessHandle
    , sandboxPort :: PortNumber
    , jsonApiPort :: PortNumber
    }

data StartCwd
    = ProjDir
    | EnvRelativeDir
    | EnvAbsoluteDir

damlStart :: StartCwd -> FilePath -> IO DamlStartResource
damlStart startCwd tmpDir = do
    let projDir = tmpDir </> "assistant-integration-tests"
    createDirectoryIfMissing True (projDir </> "daml")
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
            , "parties:"
            , "- Alice"
            , "init-script: Main:init"
            , "sandbox-options:"
            , "  - --ledgerid=MyLedger"
            , "  - --wall-clock-time"
            , "codegen:"
            , "  js:"
            , "    output-directory: ui/daml.js"
            , "    npm-scope: daml.js"
            , "  java:"
            , "    output-directory: ui/java"
            , "  scala:"
            , "    output-directory: ui/scala"
      -- this configuration option shouldn't be mandatory according to docs, but it is.
      -- See https://github.com/digital-asset/daml/issues/7547.
            , "    package-prefix: com.digitalasset"
            ]
    writeFileUTF8 (projDir </> "daml/Main.daml") $
        unlines
            [ "module Main where"
            , "import Daml.Script"
            , "template T with p : Party where signatory p"
            , "init : Script ()"
            , "init = do"
            , "  alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"Alice\")"
            , "  alice `submit` createCmd (T alice)"
            , "  pure ()"
            , "test : Int -> Script (Int, Int)"
            , "test x = pure (x, x + 1)"
            ]
    sandboxPort <- getFreePort
    jsonApiPort <- getFreePort
    let cwd
          | ProjDir <- startCwd = projDir
          | otherwise = tmpDir
    let env
          | ProjDir <- startCwd = []
          | EnvRelativeDir <- startCwd = [("DAML_PROJECT", Just "assistant-integration-tests")]
          | EnvAbsoluteDir <- startCwd = [("DAML_PROJECT", Just projDir)]
    let startProc =
            (shell $
             unwords
                 [ "daml"
                 , "start"
                 , "--start-navigator"
                 , "no"
                 , "--sandbox-port"
                 , show sandboxPort
                 , "--json-api-port"
                 , show jsonApiPort
                 ])
                {std_in = CreatePipe, cwd = Just cwd}
    withEnv env $ do
      (startStdin, _, _, startPh) <- createProcess startProc
      waitForHttpServer
          (threadDelay 100000)
          ("http://localhost:" <> show jsonApiPort <> "/v1/query")
          authorizationHeaders
      pure $
          DamlStartResource
              { projDir = projDir
              , tmpDir = tmpDir
              , sandboxPort = sandboxPort
              , jsonApiPort = jsonApiPort
              , startStdin = startStdin
              , startPh = startPh
              }

data QuickSandboxResource = QuickSandboxResource
    { quickSandboxPort :: PortNumber
    , quickProjDir :: FilePath
    , quickDar :: FilePath
    , quickSandboxPh :: ProcessHandle
    }

quickSandbox :: FilePath -> IO QuickSandboxResource
quickSandbox projDir = do
    withDevNull $ \devNull -> do
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=quickstart-java"]
        withCurrentDirectory projDir $ do
            callCommandSilent "daml build"
            sandboxPort <- getFreePort
            let sandboxProc =
                    (shell $
                     unwords
                         [ "daml"
                         , "sandbox"
                         , "--"
                         , "--port"
                         , show sandboxPort
                         , "--"
                         , "--static-time"
                         , ".daml/dist/quickstart-0.0.1.dar"
                         ])
                        {std_out = UseHandle devNull}
            (_, _, _, sandboxPh) <- createProcess sandboxProc
            waitForConnectionOnPort (threadDelay 500000) $ fromIntegral sandboxPort
            pure $
                QuickSandboxResource
                    { quickProjDir = projDir
                    , quickSandboxPort = sandboxPort
                    , quickSandboxPh = sandboxPh
                    , quickDar = projDir </> ".daml" </> "dist" </> "quickstart-0.0.1.dar"
                    }

tests :: FilePath -> TestTree
tests tmpDir =
    withSdkResource $ \_ ->
        testGroup
            "Integration tests"
            [ testCase "daml version" $
              withCurrentDirectory tmpDir $ callCommandSilent "daml version"
            , testCase "daml --help" $ withCurrentDirectory tmpDir $ callCommandSilent "daml --help"
            , testCase "daml new --list" $
              withCurrentDirectory tmpDir $ callCommandSilent "daml new --list"
            , packagingTests tmpDir
            , withResource (damlStart ProjDir tmpDir) (terminateProcess . startPh) damlStartTests
            , withResource (quickSandbox quickstartDir) (terminateProcess . quickSandboxPh) $
              quickstartTests quickstartDir mvnDir
            , cleanTests cleanDir
            , templateTests
            , codegenTests codegenDir
            ]
  where
    quickstartDir = tmpDir </> "q-u-i-c-k-s-t-a-r-t"
    cleanDir = tmpDir </> "clean"
    mvnDir = tmpDir </> "m2"
    codegenDir = tmpDir </> "codegen"

-- Most of the packaging tests are in the a separate test suite in
-- //compiler/damlc/tests:packaging. This only has a couple of
-- integration tests.
packagingTests :: FilePath -> TestTree
packagingTests tmpDir =
    testGroup
        "packaging"
        [ testCase "Build copy trigger" $ do
              let projDir = tmpDir </> "copy-trigger1"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
              withCurrentDirectory projDir $ callCommandSilent "daml build"
              let dar = projDir </> ".daml" </> "dist" </> "copy-trigger-0.0.1.dar"
              assertFileExists dar
        , testCase "Build copy trigger with LF version 1.dev" $ do
              let projDir = tmpDir </> "copy-trigger2"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
              withCurrentDirectory projDir $ callCommandSilent "daml build --target 1.dev"
              let dar = projDir </> ".daml" </> "dist" </> "copy-trigger-0.0.1.dar"
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
              withCurrentDirectory myDepDir $ callCommandSilent "daml build -o mydep.dar"
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
              withCurrentDirectory myTriggerDir $ callCommandSilent "daml build -o mytrigger.dar"
              let dar = myTriggerDir </> "mytrigger.dar"
              assertFileExists dar
        , testCase "Build DAML script example" $ do
              let projDir = tmpDir </> "script-example"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              withCurrentDirectory projDir $ callCommandSilent "daml build"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar
        , testCase "Build DAML script example with LF version 1.dev" $ do
              let projDir = tmpDir </> "script-example1"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              withCurrentDirectory projDir $ callCommandSilent "daml build --target 1.dev"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar
        , testCase "Package depending on daml-script and daml-trigger can use data-dependencies" $ do
              callCommandSilent $ unwords ["daml", "new", tmpDir </> "data-dependency"]
              withCurrentDirectory (tmpDir </> "data-dependency") $
                  callCommandSilent "daml build -o data-dependency.dar"
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
              withCurrentDirectory (tmpDir </> "proj") $ callCommandSilent "daml build"
        ]

-- We are trying to run as many tests with the same `daml start` process as possible to safe time.
damlStartTests :: IO DamlStartResource -> TestTree
damlStartTests getDamlStart =
    testGroup
        "daml start"
          -- If the ledger id or wall clock time is not picked or project dir was not found, this
          -- would fail.
        [ testCase "sandbox and json-api come up" $ do
              DamlStartResource {jsonApiPort} <- getDamlStart
              manager <- newManager defaultManagerSettings
              initialRequest <-
                  parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/create"
              let createRequest =
                      initialRequest
                          { method = "POST"
                          , requestHeaders = authorizationHeaders
                          , requestBody =
                                RequestBodyLBS $
                                Aeson.encode $
                                Aeson.object
                                    [ "templateId" Aeson..= Aeson.String "Main:T"
                                    , "payload" Aeson..= [Aeson.String "Alice"]
                                    ]
                          }
              createResponse <- httpLbs createRequest manager
              statusCode (responseStatus createResponse) @?= 200
        , testCase "daml start invokes codegen" $ do
              DamlStartResource {projDir} <- getDamlStart
              withCurrentDirectory projDir $ do
                  didGenerateJsCode <-
                      doesFileExist
                          ("ui" </> "daml.js" </> "assistant-integration-tests-1.0" </>
                           "package.json")
                  didGenerateJavaCode <-
                      doesFileExist
                          ("ui" </> "java" </> "da" </> "internal" </> "template" </> "Archive.java")
                  didGenerateScalaCode <-
                      doesFileExist
                          ("ui" </> "scala" </> "com" </> "digitalasset" </> "PackageIDs.scala")
                  didGenerateJsCode @?= True
                  didGenerateJavaCode @?= True
                  didGenerateScalaCode @?= True
        , testCase "run a daml ledger command" $ do
              DamlStartResource {projDir, sandboxPort} <- getDamlStart
              withCurrentDirectory projDir $
                  callCommand $
                  unwords ["daml", "ledger", "allocate-party", "--port", show sandboxPort, "Bob"]
        , testCase "Run init-script" $ do
              DamlStartResource {projDir, jsonApiPort} <- getDamlStart
              withCurrentDirectory projDir $ do
                  initialRequest <-
                      parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
                  let queryRequest =
                          initialRequest
                              { method = "POST"
                              , requestHeaders = authorizationHeaders
                              , requestBody =
                                    RequestBodyLBS $
                                    Aeson.encode $
                                    Aeson.object ["templateIds" Aeson..= [Aeson.String "Main:T"]]
                              }
                  manager <- newManager defaultManagerSettings
                  queryResponse <- httpLbs queryRequest manager
                  statusCode (responseStatus queryResponse) @?= 200
                  preview (key "result" . _Array . to Vector.length) (responseBody queryResponse) @?=
                      Just 2
        , testCase "DAML Script --input-file and --output-file" $ do
              DamlStartResource {projDir, sandboxPort} <- getDamlStart
              withCurrentDirectory projDir $ do
                  let dar = projDir </> ".daml" </> "dist" </> "assistant-integration-tests-1.0.dar"
                  writeFileUTF8 (projDir </> "input.json") "0"
                  callCommand $
                      unwords
                          [ "daml script"
                          , "--dar " <> dar <> " --script-name Main:test"
                          , "--input-file input.json --output-file output.json"
                          , "--ledger-host localhost --ledger-port " <> show sandboxPort
                          ]
                  contents <- readFileUTF8 (projDir </> "output.json")
                  lines contents @?= ["{", "  \"_1\": 0,", "  \"_2\": 1", "}"]
        , testCase "trigger service startup" $ do
              DamlStartResource {sandboxPort} <- getDamlStart
              withDevNull $ \devNull1 -> do
                  withDevNull $ \devNull2 -> do
                      triggerServicePort <- getFreePort
                      let triggerServiceProc =
                              (shell $
                               unwords
                                   [ "daml"
                                   , "trigger-service"
                                   , "--ledger-host"
                                   , "localhost"
                                   , "--ledger-port"
                                   , show sandboxPort
                                   , "--http-port"
                                   , show triggerServicePort
                                   , "--wall-clock-time"
                                   ])
                                  { std_out = UseHandle devNull1
                                  , std_err = UseHandle devNull2
                                  , std_in = CreatePipe
                                  }
                      withCreateProcess triggerServiceProc $ \_ _ _ triggerServicePh ->
                          race_ (waitForProcess' triggerServiceProc triggerServicePh) $ do
                              let endpoint =
                                      "http://localhost:" <> show triggerServicePort <> "/livez"
                              waitForHttpServer (threadDelay 100000) endpoint []
                              req <- parseRequest endpoint
                              manager <- newManager defaultManagerSettings
                              resp <- httpLbs req manager
                              responseBody resp @?= "{\"status\":\"pass\"}"
                              -- waitForProcess' will block on Windows so we explicitly kill the
                              -- process.
                              terminateProcess triggerServicePh
        , testCase "Navigator startup" $ do
              DamlStartResource {projDir, sandboxPort} <- getDamlStart
              withCurrentDirectory projDir $
              -- This test just checks that navigator starts up and returns a 200 response.
              -- Nevertheless this would have caught a few issues on rules_nodejs upgrades
              -- where we got a 404 instead.
               do
                  withDevNull $ \devNull -> do
                      navigatorPort :: Int <- fromIntegral <$> getFreePort
                      let navigatorProc =
                              (shell $
                               unwords
                                   [ "daml"
                                   , "navigator"
                                   , "server"
                                   , "localhost"
                                   , show sandboxPort
                                   , "--port"
                                   , show navigatorPort
                                   ])
                                  {std_out = UseHandle devNull, std_in = CreatePipe}
                      withCreateProcess navigatorProc $ \_ _ _ navigatorPh ->
                          race_ (waitForProcess' navigatorProc navigatorPh) $
                          -- waitForHttpServer will only return once we get a 200 response so we
                          -- don’t need to do anything else.
                           do
                              waitForHttpServer
                                  (threadDelay 100000)
                                  ("http://localhost:" <> show navigatorPort)
                                  []
                              -- waitForProcess' will block on Windows so we explicitly kill the
                              -- process.
                              terminateProcess navigatorPh
        , testCase "hot-reload" $ do
              DamlStartResource {projDir, jsonApiPort, startStdin} <- getDamlStart
              withCurrentDirectory projDir $ do
                  writeFileUTF8 (projDir </> "daml/Main.daml") $
                      unlines
                          [ "module Main where"
                          , "import Daml.Script"
                          , "template S with newFieldName : Party where signatory newFieldName"
                          , "init : Script ()"
                          , "init = do"
                          , "  [aliceDetails,_bob] <- listKnownParties"
                          , "  let alice = party aliceDetails"
                          , "  alice `submit` createCmd (S alice)"
                          , "  pure ()"
                          ]
                  maybe
                      (fail "No start process stdin handle")
                      (\h -> hPutChar h 'r' >> hFlush h)
                      startStdin
                  threadDelay 40_000_000
                  initialRequest <-
                      parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
                  let queryRequest =
                          initialRequest
                              { method = "POST"
                              , requestHeaders = authorizationHeaders
                              , requestBody =
                                    RequestBodyLBS $
                                    Aeson.encode $
                                    Aeson.object ["templateIds" Aeson..= [Aeson.String "Main:S"]]
                              }
                  manager <- newManager defaultManagerSettings
                  queryResponse <- httpLbs queryRequest manager
                  statusCode (responseStatus queryResponse) @?= 200
                  preview
                      (key "result" . nth 0 . key "payload" . key "newFieldName")
                      (responseBody queryResponse) @?=
                      Just "Alice"
        , testCase "daml start --sandbox-port=0" $
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
              let startProc =
                      shell $
                      unwords
                          [ "daml"
                          , "start"
                          , "--sandbox-port=0"
                          , "--sandbox-option=--ledgerid=MyLedger"
                          , "--json-api-port=0"
                          , "--json-api-option=--port-file=jsonapi.port"
                          ]
              withCurrentDirectory tmpDir $
                  withCreateProcess startProc $ \_ _ _ ph -> do
                      jsonApiPort <- readPortFile maxRetries "jsonapi.port"
                      initialRequest <-
                          parseRequest $
                          "http://localhost:" <> show jsonApiPort <> "/v1/parties/allocate"
                      let queryRequest =
                              initialRequest
                                  { method = "POST"
                                  , requestHeaders = authorizationHeaders
                                  , requestBody =
                                        RequestBodyLBS $
                                        Aeson.encode $
                                        Aeson.object ["identifierHint" Aeson..= ("Alice" :: String)]
                                  }
                      manager <- newManager defaultManagerSettings
                      queryResponse <- httpLbs queryRequest manager
                      responseBody queryResponse @?=
                          "{\"result\":{\"identifier\":\"Alice\",\"isLocal\":true},\"status\":200}"
                      -- waitForProcess' will block on Windows so we explicitly kill the process.
                      terminateProcess ph
        , testCase "daml start with relative project dir" $
          withTempDir $ \tmpDir -> do
            DamlStartResource{startPh} <- damlStart EnvRelativeDir tmpDir
            terminateProcess startPh
        , testCase "daml start absolut path" $
          withTempDir $ \tmpDir -> do
            DamlStartResource{startPh} <- damlStart EnvAbsoluteDir tmpDir
            terminateProcess startPh
        ]

quickstartTests :: FilePath -> FilePath -> IO QuickSandboxResource -> TestTree
quickstartTests quickstartDir mvnDir getSandbox =
    testGroup
        "quickstart"
        [ testCase "daml test" $ withCurrentDirectory quickstartDir $ callCommandSilent "daml test"
        -- Testing `daml new` and `daml build` is done when the QuickSandboxResource is build.
        , testCase "daml damlc test --files" $
          withCurrentDirectory quickstartDir $
          callCommandSilent "daml damlc test --files daml/Main.daml"
        , testCase "daml damlc visual-web" $
          withCurrentDirectory quickstartDir $
          callCommandSilent $
          unwords ["daml damlc visual-web .daml/dist/quickstart-0.0.1.dar -o visual.html -b"]
        , testCase "mvn compile" $
          withCurrentDirectory quickstartDir $ do
              mvnDbTarball <-
                  locateRunfiles
                      (mainWorkspace </> "daml-assistant" </> "integration-tests" </>
                       "integration-tests-mvn.tar")
              runConduitRes $
                  sourceFileBS mvnDbTarball .|
                  Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError mvnDir)
              callCommand "daml codegen java"
              callCommand $ unwords ["mvn", mvnRepoFlag, "-q", "compile"]
        , testCase "Sandbox Classic startup" $
          withCurrentDirectory quickstartDir $
          withDevNull $ \devNull -> do
              p :: Int <- fromIntegral <$> getFreePort
              let sandboxProc =
                      (shell $
                       unwords
                           [ "daml"
                           , "sandbox-classic"
                           , "--wall-clock-time"
                           , "--port"
                           , show p
                           , ".daml/dist/quickstart-0.0.1.dar"
                           ])
                          {std_out = UseHandle devNull, std_in = CreatePipe}
              withCreateProcess sandboxProc $ \_ _ _ ph ->
                  race_ (waitForProcess' sandboxProc ph) $ do
                      waitForConnectionOnPort (threadDelay 100000) p
                      addr:_ <- getAddrInfo (Just socketHints) (Just "127.0.0.1") (Just $ show p)
                      bracket
                          (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
                          close
                          (\s -> connect s (addrAddress addr))
              -- waitForProcess' will block on Windows so we explicitly kill the process.
                      terminateProcess ph
        , testCase "mvn exec:java@run-quickstart" $ do
              QuickSandboxResource {quickProjDir, quickSandboxPort, quickDar} <- getSandbox
              withCurrentDirectory quickProjDir $
                  withDevNull $ \devNull -> do
                      callCommandSilent $
                          unwords
                              [ "daml script"
                              , "--dar " <> quickDar
                              , "--script-name Main:initialize"
                              , "--static-time"
                              , "--ledger-host localhost"
                              , "--ledger-port"
                              , show quickSandboxPort
                              ]
                      restPort :: Int <- fromIntegral <$> getFreePort
                      let mavenProc =
                              (shell $
                               unwords
                                   [ "mvn"
                                   , mvnRepoFlag
                                   , "-Dledgerport=" <> show quickSandboxPort
                                   , "-Drestport=" <> show restPort
                                   , "exec:java@run-quickstart"
                                   ])
                                  {std_out = UseHandle devNull}
                      withCreateProcess mavenProc $ \_ _ _ mavenPh ->
                          race_ (waitForProcess' mavenProc mavenPh) $ do
                              let url = "http://localhost:" <> show restPort <> "/iou"
                              waitForHttpServer (threadDelay 1000000) url []
                              threadDelay 5000000
                              manager <- newManager defaultManagerSettings
                              req <- parseRequest url
                              req <-
                                  pure req {requestHeaders = [(hContentType, "application/json")]}
                              resp <- httpLbs req manager
                              responseBody resp @?=
                                  "{\"0\":{\"issuer\":\"EUR_Bank\",\"owner\":\"Alice\",\"currency\":\"EUR\",\"amount\":100.0000000000,\"observers\":[]}}"
                  -- waitForProcess' will block on Windows so we explicitly kill the process.
                              terminateProcess mavenPh
        ]
  where
    mvnRepoFlag = "-Dmaven.repo.local=" <> mvnDir

-- | Ensure that daml clean removes precisely the files created by daml build.
cleanTests :: FilePath -> TestTree
cleanTests baseDir = testGroup "daml clean"
    [ cleanTestFor "skeleton"
    , cleanTestFor "quickstart-java"
    , cleanTestFor "quickstart-scala"
    ]
    where
        cleanTestFor :: String -> TestTree
        cleanTestFor templateName =
            testCase ("daml clean test for " <> templateName <> " template") $ do
                createDirectoryIfMissing True baseDir
                withCurrentDirectory baseDir $ do
                    let projectDir = baseDir </> ("proj-" <> templateName)
                    callCommandSilent $ unwords ["daml", "new", projectDir, "--template", templateName]
                    withCurrentDirectory projectDir $ do
                        filesAtStart <- sort <$> listFilesRecursive "."
                        callCommandSilent "daml build"
                        callCommandSilent "daml clean"
                        filesAtEnd <- sort <$> listFilesRecursive "."
                        when (filesAtStart /= filesAtEnd) $
                            fail $ unlines
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
          callCommandSilent $ unwords ["daml", "new", dir, "--template", name]
          withCurrentDirectory dir $ callCommandSilent "daml build"
    | name <- templateNames
    ] <>
    [ testCase "quickstart-java, positional template" $ do
          withTempDir $ \tmpDir -> do
          let dir = tmpDir </> "foobar"
          -- Verify that the old syntax for `daml new` still works.
          callCommandSilent $ unwords ["daml","new", dir, "quickstart-java"]
          contents <- readFileUTF8 $ dir </> "daml.yaml"
          assertInfixOf "name: quickstart" contents
    ]
  -- NOTE (MK) We might want to autogenerate this list at some point but for now
  -- this should be good enough.
  where templateNames =
            [ "copy-trigger"
            -- daml-intro-1 - daml-intro-6 are not full projects.
            , "daml-intro-7"
            , "daml-patterns"
            , "quickstart-java"
            , "quickstart-scala"
            , "script-example"
            , "skeleton"
            , "create-daml-app"
            ]

-- | Check we can generate language bindings.
codegenTests :: FilePath -> TestTree
codegenTests codegenDir = testGroup "daml codegen" (
    [ codegenTestFor "java" Nothing
    , codegenTestFor "scala" (Just "com.cookiemonster.nomnomnom")
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
                withCurrentDirectory codegenDir $ do
                    let projectDir = codegenDir </> ("proj-" ++ lang)
                    callCommandSilent $ unwords ["daml new", projectDir, "--template=skeleton"]
                    withCurrentDirectory projectDir $ do
                        callCommandSilent "daml build"
                        let darFile = projectDir </> ".daml/dist/proj-" ++ lang ++ "-0.0.1.dar"
                            outDir  = projectDir </> "generated" </> lang
                        when (lang == "js") $ do
                            let workspaces = Workspaces [makeRelative codegenDir outDir]
                            setupYarnEnv codegenDir workspaces [DamlTypes, DamlLedger]
                        callCommandSilent $
                          unwords [ "daml", "codegen", lang
                                  , darFile ++ maybe "" ("=" ++) namespace
                                  , "-o", outDir]
                        contents <- listDirectory outDir
                        assertBool "bindings were written" (not $ null contents)

-- | Like `waitForProcess` but throws ProcessExitFailure if the process fails to start.
waitForProcess' :: CreateProcess -> ProcessHandle -> IO ()
waitForProcess' cp ph = do
    e <- waitForProcess ph
    unless (e == ExitSuccess) $ throwIO $ ProcessExitFailure e cp

data ProcessExitFailure = ProcessExitFailure !ExitCode !CreateProcess
    deriving (Show, Typeable)

instance Exception ProcessExitFailure

