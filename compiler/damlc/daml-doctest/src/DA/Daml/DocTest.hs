-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.DocTest
    ( getDocTestModule
    , docTestModuleName
    , GeneratedModule(..)
    ) where

import Control.DeepSeq
import Data.Char
import Data.Generics hiding (Generic)
import Data.List
import Data.Text (Text)
import qualified Data.Text as T
import GHC hiding (parseModule)
import GHC.Generics (Generic)

-- Most of this code is a variation of the code in Haskell’s doctest, in particular
-- the [Extract](https://github.com/sol/doctest/blob/master/src/Extract.hs) module for
-- extracting doc comments from a `ParsedModule` and the
-- [Parse](https://github.com/sol/doctest/blob/master/src/Parse.hs) module for
-- extracting tests from doc comments.
-- Our requirements are somewhat different (e.g. we don’t use actual GHCi commands)
-- so we roll our own version.

----------------------------------------------
-- Types shared across the different stages --
----------------------------------------------

data DocTestModule a = DocTestModule
    { dtModuleName :: Text
    , dtModuleContent :: [a]
    -- For now, we do not handle setup but we probably will
    -- have to handle it at some point
    } deriving (Eq, Show, Functor)

--------------------------------
-- Extraction of doc comments --
--------------------------------

extractFromModule :: ParsedModule -> DocTestModule LHsDocString
extractFromModule m = DocTestModule name (map snd docs)
    where
        name = T.pack . moduleNameString . GHC.moduleName . ms_mod . pm_mod_summary $ m
        docs = docStringsFromModule m

-- | We return a tuple of the doc comment name (if any) and the actual doc.
-- The name allows us to differentiate between setup and regular doc comments.
docStringsFromModule :: ParsedModule -> [(Maybe String, LHsDocString)]
docStringsFromModule mod = docs
      where
          -- TODO We might want to mine docs from exports and the header as well.
          docs = decls
          source = unLoc . pm_parsed_source $ mod
          decls = extractDocStrings $ hsmodDecls source

-- | Extract all docstrings from given value.
extractDocStrings :: Data a => a -> [(Maybe String, LHsDocString)]
extractDocStrings = everythingBut (++) (([], False) `mkQ` fromLHsDecl
    `extQ` fromLDocDecl
    `extQ` fromLHsDocString
    )
  where
    fromLHsDecl :: Selector (LHsDecl GhcPs)
    fromLHsDecl (L loc decl) = case decl of
        -- Top-level documentation has to be treated separately, because it has
        -- no location information attached.  The location information is
        -- attached to HsDecl instead.
        DocD _ x -> select (fromDocDecl loc x)
        _ -> (extractDocStrings decl, True)

    fromLDocDecl :: Selector LDocDecl
    fromLDocDecl (L loc x) = select (fromDocDecl loc x)

    fromLHsDocString :: Selector LHsDocString
    fromLHsDocString x = select (Nothing, x)

    fromDocDecl :: SrcSpan -> DocDecl -> (Maybe String, LHsDocString)
    fromDocDecl loc x = case x of
        DocCommentNamed name doc -> (Just name, L loc doc)
        _                        -> (Nothing, L loc $ docDeclDoc x)

type Selector a = a -> ([(Maybe String, LHsDocString)], Bool)
select :: a -> ([a], Bool)
select x = ([x], False)

---------------------------------------
-- Parse doc comments into doc tests --
---------------------------------------

data DocTest = DocTest
    { dtExpr :: Text
    , dtExpectedResult :: [Text]
    }

parseModule :: DocTestModule LHsDocString -> DocTestModule (Located DocTest)
parseModule m = m { dtModuleContent = concatMap parseDocTests $ dtModuleContent m }

parseDocTests :: LHsDocString -> [Located DocTest]
parseDocTests docString =
    map (L (getLoc docString)) .  go . T.lines . T.pack . unpackHDS . unLoc $ docString
  where
    isPrompt :: Text -> Bool
    isPrompt = T.isPrefixOf ">>>" . T.dropWhile isSpace
    dropPromptPrefix = T.drop 3 . T.dropWhile isSpace

    isBlankLine :: Text -> Bool
    isBlankLine  = T.all isSpace

    isCodeBlockEnd :: Text -> Bool
    isCodeBlockEnd = T.isInfixOf "```"

    isEndOfInteraction :: Text -> Bool
    isEndOfInteraction x = isPrompt x || isBlankLine x || isCodeBlockEnd x

    go :: [Text] -> [DocTest]
    go xs = case dropWhile (not . isPrompt) xs of
        prompt:rest ->
            let (ys,zs) = break isEndOfInteraction rest
            in toDocTest (T.strip $ dropPromptPrefix prompt) ys : go zs
        [] -> []

toDocTest :: Text -> [Text] -> DocTest
toDocTest = DocTest

---------------------------
-- Render doctest module --
---------------------------

-- | Identifier for a doctest. This allows us to map
-- the doctest back to the source in the original module.
newtype DocTestId = DocTestId Int
    deriving (Eq, Ord, Show, Enum)

renderDocTestModule :: DocTestModule (Located DocTest) -> Text
renderDocTestModule DocTestModule{..} = rendered
    where
        testsWithIds = zip [DocTestId 0..] dtModuleContent
        rendered = T.unlines $
            [ "{-# OPTIONS_GHC -Wno-unused-imports #-}"
            , "module " <> docTestModuleName dtModuleName <> " where"
            , ""
            , "import " <> dtModuleName
            , "import DA.Assert"
            , "import GHC.Types"
            , ""
            ] <>
            intercalate [""] (map (uncurry renderDocTest) testsWithIds)

renderDocTest :: DocTestId -> Located DocTest -> [Text]
renderDocTest (DocTestId i) (unLoc -> DocTest{..}) =
    [ "doctest_" <> T.pack (show i) <> " = " <> "scenario do"
    , "  (===) (" <> dtExpr <> ") $"
    ] <>
    map (indent 4) dtExpectedResult
    where
        indent i t = T.replicate i " " <> t

getDocTestModule :: ParsedModule -> GeneratedModule
getDocTestModule pm = case parseModule $ extractFromModule pm of
    m -> GeneratedModule (dtModuleName m) (renderDocTestModule m)

docTestModuleName :: Text -> Text
docTestModuleName t = t <> "_doctest"

data GeneratedModule = GeneratedModule
  { genModuleName :: Text
  , genModuleContent :: Text
  } deriving (Show, Generic)

instance NFData GeneratedModule
