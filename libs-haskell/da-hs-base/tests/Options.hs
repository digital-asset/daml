-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options(main) where

import Test.Tasty
import Test.Tasty.HUnit
import Options.Applicative.Extended
import Options.Applicative

main :: IO ()
main = defaultMain $ testGroup "Extended CLI options" [singleFlagParse, multiFlagParse, someAndManyParse]

parser :: ParserInfo String
parser = info (optionOnce str (long "opt" <> short 'o')) mempty

parserMany :: ParserInfo [String]
parserMany = info (many $ optionOnce str (long "opt" <> short 'o')) mempty

parserSome :: ParserInfo [String]
parserSome = info (some $ optionOnce str (long "opt" <> short 'o')) mempty

assertParseSuccess :: (Eq b, Show b) => b -> ParserResult b -> Assertion
assertParseSuccess expected (Success actual) = assertEqual "" expected actual
assertParseSuccess expected (Failure err) = assertFailure $ "Expected " <> show expected <> ", got: " <> show err
assertParseSuccess expected (CompletionInvoked _) = assertFailure $ "Expected " <> show expected <> ", completion invoked instead."

assertParseFailure :: Show b => ParserResult b -> Assertion
assertParseFailure (Success actual) = assertFailure $ "Expected flag parse failure, got successful flag parse of value " <> show actual <> " instead."
assertParseFailure (Failure _) = pure ()
assertParseFailure (CompletionInvoked _) = assertFailure "Expected flag parse failure, completion invoked instead."

singleFlagParse :: TestTree
singleFlagParse =
  testGroup "single flag parse is valid"
    [ testCase "long flag with argument separate word succeeds" $
        assertParseSuccess "value" $
          execParserPure defaultPrefs parser ["--opt", "value"]
    , testCase "long flag with argument same word succeeds" $
        assertParseSuccess "value" $
          execParserPure defaultPrefs parser ["--opt=value"]
    , testCase "short flag with argument separate word succeeds" $
        assertParseSuccess "value" $
          execParserPure defaultPrefs parser ["-o", "value"]
    , testCase "short flag with argument same word succeeds" $
        assertParseSuccess "value" $
          execParserPure defaultPrefs parser ["-ovalue"]
    ]

multiFlagParse :: TestTree
multiFlagParse =
  testGroup "double flag parse is invalid"
    [ testCase "two long-style occurrences of flag fails" $
        assertParseFailure $
          execParserPure defaultPrefs parser ["--opt", "value", "--opt", "value"]
    , testCase "two short-style occurrences of flag fails" $
        assertParseFailure $
          execParserPure defaultPrefs parser ["-ovalue", "--opt value"]
    , testCase "three long-style occurrences of flag fails" $
        assertParseFailure $
          execParserPure defaultPrefs parser ["--opt", "value", "--opt", "value", "--opt", "value"]
    ]

someAndManyParse :: TestTree
someAndManyParse =
  testGroup "single flag parse is valid"
    [ testCase "many succeeds with no arguments" $
        assertParseSuccess [] $
          execParserPure defaultPrefs parserMany []
    , testCase "many fails when flag is passed twice" $
        assertParseFailure $
          execParserPure defaultPrefs parserMany ["--opt", "value", "-ovalue", "-o", "value"]
    , testCase "some modifier fails when flag is passed twice" $
        assertParseFailure $
          execParserPure defaultPrefs parserSome ["--opt", "value", "-ovalue", "-o", "value"]
    ]


