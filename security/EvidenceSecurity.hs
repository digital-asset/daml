-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Monad (when)
import Data.List (intercalate, (\\), sortOn)
import Data.List.Extra (trim,groupOn)
import Data.List.Split (splitOn)
import Data.Map (Map)
import System.Exit (exitWith,ExitCode(ExitSuccess,ExitFailure))
import System.FilePath (splitPath)
import System.IO.Extra (hPutStrLn,stderr)
import Text.Read (readMaybe)
import qualified Data.Map as Map (fromList,toList)

{-
Generate _security evidence_ by documenting _security_ test cases.

Security tests may be found anywhere in the Daml repository, and written in any language
(scala, haskell, shell, etc). They are marked by the *magic comment*: "SECURITY_TEST"
followed by a ":".

Following the marker, the remaining text on the line is split on the next ":" to give:
        Category : Free text description of the test case.

There are a fixed set of categories, listed in the enum below. There expect at least one
testcase for every category.

The generated evidence is a markdown file, listing each testcase, grouped by Category. For
each testcase we note the free-text with a link to the line in the original file.

This program is expected to be run with stdin generated by a git grep command, and stdout
redirected to the name of the generated file:

```
git grep --line-number SECURITY_TEST\: | bazel run security:evidence-security > security-evidence.md
```
-}

main :: IO ()
main = do
  rawLines <- lines <$> getContents
  let parsed = map parseLine rawLines
  let lines = [ line | Right line <- parsed ]
  let cats = [ cat | Line{cat} <- lines ]
  let missingCats = [minBound ..maxBound] \\ cats
  let errs = [ err | Left err <- parsed ] ++ map NoTestsForCategory missingCats
  let n_errs = length errs
  when (n_errs >= 1) $ do
    hPutStrLn stderr "** Errors while Evidencing Security; exiting with non-zero exit code."
    sequence_ [hPutStrLn stderr $ "** (" ++ show i ++ ") " ++ formatError err | (i,err) <- zip [1::Int ..] errs]
  putStrLn (ppCollated (collateLines lines))
  exitWith (if n_errs == 0 then ExitSuccess else ExitFailure n_errs)

data Category = Authorization | Privacy | Semantics | Performance
  deriving (Eq,Ord,Bounded,Enum,Show)

data Description = Description
  { filename:: FilePath
  , lineno:: Int
  , freeText:: String
  }

data Line = Line { cat :: Category, desc :: Description }

newtype Collated = Collated (Map Category [Description])

data Err
  = FailedToSplitLineOn4colons String
  | FailedToParseLinenumFrom String String
  | UnknownCategoryInLine String String
  | NoTestsForCategory Category

parseLine :: String -> Either Err Line
parseLine lineStr = do
  let sep = ":"
  case splitOn sep lineStr of
    filename : linenoString : _magicComment_ : tag : rest@(_:_) -> do
      case categoryFromTag (trim tag) of
        Nothing -> Left (UnknownCategoryInLine (trim tag) lineStr)
        Just cat -> do
          case readMaybe @Int linenoString of
            Nothing -> Left (FailedToParseLinenumFrom linenoString lineStr)
            Just lineno -> do
              let freeText = trim (intercalate sep rest)
              let desc = Description {filename,lineno,freeText}
              Right (Line {cat,desc})
    _ ->
      Left (FailedToSplitLineOn4colons lineStr)

categoryFromTag :: String -> Maybe Category
categoryFromTag = \case
  "Authorization" -> Just Authorization
  "Privacy" -> Just Privacy
  "Semantics" -> Just Semantics
  "Performance" -> Just Performance
  _ -> Nothing

collateLines :: [Line] -> Collated
collateLines lines =
  Collated $ Map.fromList
  [ (cat, [ desc | Line{desc} <- group ])
  | group@(Line{cat}:_) <- groupOn (\Line{cat} -> cat) lines
  ]

formatError :: Err -> String
formatError = \case
  FailedToSplitLineOn4colons line -> "failed to parse line (expected 4 colons): " ++ line
  FailedToParseLinenumFrom s line -> "failed to parse line-number from '" ++ show s ++ "' in line: " ++ line
  UnknownCategoryInLine s line -> "unknown category '" ++ s ++ "' in line: " ++ line
  NoTestsForCategory cat -> "no tests found for category: " ++ show cat

ppCollated :: Collated -> String
ppCollated (Collated m) =
  unlines (["# Security tests, by category",""] ++
           [ unlines (("## " ++ ppCategory cat ++ ":") : map ppDescription (sortOn freeText descs))
           | (cat,descs) <- sortOn fst (Map.toList m)
           ])

ppDescription :: Description -> String
ppDescription Description{filename,lineno,freeText} =
  "- " ++ freeText ++  ": [" ++ basename filename ++ "](" ++ filename ++ "#L" ++ show lineno ++ ")"
  where
    basename :: FilePath -> FilePath
    basename p = case reverse (splitPath p) of [] -> ""; x:_ -> x

ppCategory :: Category -> String
ppCategory = \case
  Authorization -> "Authorization"
  Privacy -> "Privacy"
  Semantics -> "Semantics"
  Performance -> "Performance"
