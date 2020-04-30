-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Repl.FuncTests (main) where

import Control.Concurrent
import Control.Concurrent.Async
-- import Control.Concurrent.Chan
import Control.Exception
import Control.Monad.Extra
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging
import DA.Cli.Output
import DA.Daml.Compiler.Repl
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Types
import Data.Either
import DA.Test.Sandbox
import DA.Test.Util
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Types.Location
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import GHC.IO.Handle
import SdkVersion
import System.Directory
import System.FilePath
import System.IO.Extra
import System.IO.Silently
import System.Process
import Test.Hspec
import Text.Regex.TDFA

-- NOTE (MK) This test suite tests the repl as a library rather than creating a separate
-- process for each test. This is significantly faster but comes with a few challenges:
--
-- 1. repline (and for the most part haskeline) do not provide a way to supply custom handles
-- 2. This means that we have to temporarily redirect stdin and stdout to capture them.
-- 3. Tasty interleaves its own output with the output of tests. This breaks any attempts
--    at capturing stdout.
-- 4. hspec in single-threaded mode luckily works as we want to so for now, this is our only
--    hspec test suite. Another option would be to write a reporter for tasty that prints to
--    stderr.

main :: IO ()
main = do
    setNumCapabilities 1
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test.dar")
    replDir <- locateRunfiles (mainWorkspace </> "compiler/repl-service/server")
    forM_ [stdin, stdout, stderr] $ \h -> hSetBuffering h LineBuffering
    let replJar = replDir </> "repl-service.jar"
    -- We have to keep draining the service stdout otherwise it might get blocked
    -- if the buffer is full. Therefore we write asynchronously to a Chan
    -- and read from there.
    serviceLineChan <- newChan
    -- Turning the repl client into a resource is a bit annoying since it requires
    -- pulling apart functions like `withGrpcClient`. Therefore we just
    -- allocate the resources before handing over to tasty and accept that
    -- it will spin up sandbox and the repl client.
    withTempFile $ \portFile ->
        withBinaryFile nullDevice WriteMode $ \devNull ->
        bracket (createSandbox portFile devNull defaultSandboxConf { dars = [testDar] }) destroySandbox $ \SandboxResource{sandboxPort} ->
        ReplClient.withReplClient (ReplClient.Options replJar "localhost" (show sandboxPort) Nothing Nothing CreatePipe) $ \replHandle mbServiceOut processHandle ->
        -- TODO We could share some of this setup with the actual repl code in damlc.
        withTempDir $ \dir ->
        withCurrentDirectory dir $ do
        Just serviceOut <- pure mbServiceOut
        hSetBuffering serviceOut LineBuffering
        withAsync (drainHandle serviceOut serviceLineChan) $ \_ -> do
            initPackageConfig scriptDar testDar
            logger <- Logger.newStderrLogger Logger.Warning "repl-tests"
            withDamlIdeState options logger (hDiagnosticsLogger stdout) $ \ideState ->
                (hspec $ functionalTests replHandle serviceLineChan testDar options ideState) `finally`
                    -- We need to kill the process to avoid getting stuck in hGetLine on Windows.
                    terminateProcess processHandle

initPackageConfig :: FilePath -> FilePath -> IO ()
initPackageConfig scriptDar testDar = do
    writeFileUTF8 "daml.yaml" $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: repl"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies:"
        , "- daml-prim"
        , "- daml-stdlib"
        , "- " <> show scriptDar
        , "- " <> show testDar
        ]
    withPackageConfig (ProjectPath ".") $ \PackageConfigFields {..} -> do
        dir <- getCurrentDirectory
        createProjectPackageDb (toNormalizedFilePath' dir) options pSdkVersion pDependencies pDataDependencies

drainHandle :: Handle -> Chan String -> IO ()
drainHandle handle chan = forever $ do
    line <- hGetLine handle
    writeChan chan line

options :: Options
options = (defaultOptions Nothing) { optScenarioService = EnableScenarioService False }

functionalTests :: ReplClient.Handle -> Chan String -> FilePath -> Options -> IdeState -> Spec
functionalTests replClient serviceOut testDar options ideState = describe "repl func tests" $ sequence_
    [ testInteraction' "create and query"
          [ input "alice <- allocateParty \"Alice\""
          , input "debug =<< query @T alice"
          , matchServiceOutput "^.*: \\[\\]$"
          , input "submit alice $ createCmd (T alice alice)"
          , input "debug =<< query @T alice"
          , matchServiceOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          ]
    , testInteraction' "propose and accept"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "submit alice $ createCmd (TProposal alice bob)"
          , input "props <- query @TProposal bob"
          , input "debug props"
          , matchServiceOutput "^.*: \\[\\(<contract-id>,TProposal {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "forA props $ \\(prop, _) -> submit bob $ exerciseCmd prop Accept"
          , input "debug =<< query @T bob"
          , matchServiceOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "debug =<< query @TProposal bob"
          , matchServiceOutput "^.*: \\[\\]$"
          ]
    , testInteraction' "shadowing"
          [ input "x <- pure 1"
          , input "debug x"
          , matchServiceOutput "^.*: 1$"
          , input "x <- pure $ x + x"
          , input "debug x"
          , matchServiceOutput "^.*: 2$"
          ]
    , testInteraction' "parse error"
          [ input "eaiu\\1"
          , matchOutput "^parse error.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "Tuple patterns"
          [ input "(a, b) <- pure (1, 2)"
          , input "(b, c) <- pure (3, 4)"
          , input "debug a"
          , matchServiceOutput "^.*: 1"
          , input "debug b"
          , matchServiceOutput "^.*: 3"
          , input "debug c"
          , matchServiceOutput "^.*: 4"
          ]
    , testInteraction' "Partial patterns"
          -- TODO (MK) We do not test failing patterns yet
          -- since `error` calls aren’t handled nicely atm.
          [ input "Some (x, Some y) <- pure (Some (1, Some 2))"
          , input "debug x"
          , matchServiceOutput "^.*: 1"
          , input "debug y"
          , matchServiceOutput "^.*: 2"
          ]
    , testInteraction' "type error"
          [ input "1"
          -- TODO Make this less noisy
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*error.*$"
          , matchOutput "^.*expected type .*Script .* with actual type .*Int.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "script error"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "submit alice (createCmd (T alice bob))"
          , matchServiceOutput "^.*Submit failed.*requires authorizers.*but only.*were given.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "server error"
          [ input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , matchServiceOutput "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid argument: Party already exists"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "module imports"
          [ input "debug (days 1)"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*error: Variable not in scope: days.*$"
          , input "import DA.Time.Misspelled"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*Could not find module.*$"
          , matchOutput "^.*It is not a module.*$"
          , input "import DA.Time"
          , input "debug (days 1)"
          , matchServiceOutput "^.*: RelTime {microseconds = 86400000000}$"
          ]
    , testInteraction' "error call"
          [ input "error \"foobar\""
          , matchServiceOutput "^Error: User abort: foobar$"
          ]
    , testInteraction' "abort call"
          [ input "abort \"foobar\""
          , matchServiceOutput "^Error: User abort: foobar$"
          ]
    ]
  where
    testInteraction' testName steps =
        it testName $
        testInteraction testDar replClient serviceOut options ideState steps

testInteraction
    :: FilePath
    -> ReplClient.Handle
    -> Chan String
    -> Options
    -> IdeState
    -> [Step]
    -> Expectation
testInteraction testDar replClient serviceOut options ideState steps = do
    let (inLines, outAssertions) = processSteps steps
    -- On Windows we cannot dup2 between a file handle and a pipe.
    -- Therefore, we redirect to files, run the action and assert afterwards.
    out <- withTempFile $ \stdinFile -> do
        writeFileUTF8 stdinFile (unlines inLines)
        withBinaryFile stdinFile ReadMode $ \readIn ->
            redirectingHandle stdin readIn $ do
            Right () <- ReplClient.clearResults replClient
            capture_ $ runRepl options testDar replClient ideState
    -- Write output to a file so we can conveniently read individual characters.
    withTempFile $ \clientOutFile -> do
        writeFileUTF8 clientOutFile out
        withBinaryFile clientOutFile ReadMode $ \clientOut ->
            forM_ outAssertions $ \case
                Prompt -> readPrompt clientOut
                MatchRegex regex regexStr producer -> do
                    line <- case producer of
                        ReplClient -> hGetLine clientOut
                        ReplService -> readChan serviceOut
                    unless (matchTest regex line) $
                        expectationFailure (show line <> " did not match " <> show regexStr)

-- Walk over the steps separating them into the input to the repl and the output assertions.
processSteps :: [Step] -> ([String], [OutputAssertion])
processSteps = partitionEithers . concatMap f
  where f (Input s) = [Left s, Right Prompt]
        f (MatchOutput a b c) = [Right (MatchRegex a b c)]

data OutputAssertion = Prompt | MatchRegex Regex String Producer

data Step
    = Input String
    -- ^ Input a line into the repl
    | MatchOutput Regex String Producer
    -- ^ Match a line of output against a given regex.
    -- The String is used for error messages since Regex
    -- does not have a Show instance.

data Producer = ReplClient | ReplService

input :: String -> Step
input = Input

matchOutput :: String -> Step
matchOutput s = MatchOutput (makeRegex s) s ReplClient

matchServiceOutput :: String -> Step
matchServiceOutput s = MatchOutput (makeRegex s) s ReplService

readPrompt :: Handle -> Expectation
readPrompt h = do
    res <- replicateM 6 (hGetChar h)
    res `shouldBe` "daml> "

-- | Based on similar functions in @main-tester@ and @silently@
--
-- We need to redirect stdin which those packages don't provide for us.
redirectingHandle :: Handle -> Handle -> IO r -> IO r
redirectingHandle from to action = do
  let redirect = do
        save <- hDuplicate from
        hDuplicateTo to from
        return save
      restore save = do
        hDuplicateTo save from
        hClose save
  bracket redirect restore (const action)
