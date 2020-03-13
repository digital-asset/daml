-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Repl (main) where

import Control.Monad.Extra
import DA.Bazel.Runfiles
import DA.Test.Sandbox
import Data.Foldable
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    testDar <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> "repl-test.dar")
    defaultMain $ withSandbox defaultSandboxConf { dars = [testDar] } $ \getSandboxPort ->
        let testInteraction' testName steps =
                testCase testName $ do
                    p <- getSandboxPort
                    testInteraction damlc p scriptDar testDar steps
        in testGroup "repl"
            [ testInteraction' "create and query"
                  [ input "alice <- allocateParty \"Alice\""
                  , input "debug =<< query @T alice"
                  , matchOutput "^.*: \\[\\]$"
                  , input "submit alice $ createCmd (T alice alice)"
                  , input "debug =<< query @T alice"
                  , matchOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
                  ]
            , testInteraction' "propose and accept"
                  [ input "alice <- allocateParty \"Alice\""
                  , input "bob <- allocateParty \"Bob\""
                  , input "submit alice $ createCmd (TProposal alice bob)"
                  , input "props <- query @TProposal bob"
                  , input "debug props"
                  , matchOutput "^.*: \\[\\(<contract-id>,TProposal {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
                  , input "forA props $ \\(prop, _) -> submit bob $ exerciseCmd prop Accept"
                  , input "debug =<< query @T bob"
                  , matchOutput "^.*: \\[\\(<contract-id>,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
                  , input "debug =<< query @TProposal bob"
                  , matchOutput "^.*: \\[\\]$"
                  ]
            , testInteraction' "shadowing"
                  [ input "x <- pure 1"
                  , input "debug x"
                  , matchOutput "^.*: 1$"
                  , input "x <- pure $ x + x"
                  , input "debug x"
                  , matchOutput "^.*: 2$"
                  ]
            , testInteraction' "parse error"
                  [ input "eaiu\\1"
                  , matchOutput "^parse error.*$"
                  , input "debug 1"
                  , matchOutput "^.*: 1"
                  ]
            , testInteraction' "unsupported statement"
                  [ input "(x, y) <- pure (1, 2)"
                  , matchOutput "^Unsupported statement:.*$"
                  , input "debug 1"
                  , matchOutput "^.*: 1"
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
                  , matchOutput "^.*expected type .*Script _.* with actual type .*Int.*$"
                  , matchOutput "^.*$"
                  , matchOutput "^.*$"
                  , matchOutput "^.*$"
                  , matchOutput "^.*$"
                  , input "debug 1"
                  , matchOutput "^.*: 1"
                  ]
            , testInteraction' "script error"
                  [ input "alice <- allocateParty \"Alice\""
                  , input "bob <- allocateParty \"Bob\""
                  , input "submit alice (createCmd (T alice bob))"
                  , matchOutput "^.*Submit failed.*requires authorizers.*but only.*were given.*$"
                  , input "debug 1"
                  , matchOutput "^.*: 1"
                  ]
            ]

testInteraction :: FilePath -> Int -> FilePath -> FilePath -> [Step] -> Assertion
testInteraction damlc ledgerPort scriptDar testDar steps = withCreateProcess cp $ \mbIn mbOut _mbErr ph -> do
    Just hIn <- pure mbIn
    Just hOut <- pure mbOut
    for_ [hIn, hOut] $ \h -> hSetBuffering h LineBuffering
    for_ steps $ \step -> do
        case step of
            Input s -> do
                readPrompt hOut
                hPutStrLn hIn s
            MatchOutput regex regexStr -> do
                line <- hGetLine hOut
                assertBool
                    (show line <> " did not match " <> show regexStr)
                    (matchTest regex line)
    hClose hIn
    exit <- waitForProcess ph
    exit @?= ExitSuccess
    where cp = (proc damlc
              [ "repl"
              , "--ledger-host=localhost"
              , "--ledger-port"
              , show ledgerPort
              , "--script-lib"
              , scriptDar
              , testDar
              ]) { std_in = CreatePipe, std_out = CreatePipe }

data Step
    = Input String
    -- ^ Input a line into the repl
    | MatchOutput Regex String
    -- ^ Match a line of output against a given regex.
    -- The String is used for error messages since Regex
    -- does not have a Show instance.

input :: String -> Step
input = Input

matchOutput :: String -> Step
matchOutput s = MatchOutput (makeRegex s) s

readPrompt :: Handle -> Assertion
readPrompt h = do
    res <- replicateM 6 (hGetChar h)
    res @?= "daml> "
