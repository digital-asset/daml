-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Render.Tests(mkTestTree)
  where

import           DA.Daml.Doc.Types
import           DA.Daml.Doc.Render

import           Control.Monad.Except
import Data.Text qualified as T
import Data.Text.IO qualified as T
import           Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Map.Merge.Strict qualified as Map.Merge

import Test.Tasty.Extended qualified as Tasty
import           Test.Tasty.HUnit


mkTestTree :: AnchorMap -> IO Tasty.TestTree
mkTestTree externalAnchors = do
  pure $ Tasty.testGroup "DA.Daml.Doc.Render"
    [ Tasty.testGroup "RST Rendering" $
      zipWith (renderTest Rst externalAnchors) cases (expectRst False)
    , Tasty.testGroup "Markdown Rendering" $
      zipWith (renderTest Markdown externalAnchors) cases expectMarkdown
    , Tasty.testGroup "RST Folder Rendering" $ pure $
      renderFolderTest Rst externalAnchors folderTestCase expectRstFolder
    , Tasty.testGroup "Markdown Folder Rendering" $ pure $
      renderFolderTest Markdown externalAnchors folderTestCase expectMarkdownFolder
    ]
  where
    folderTestCase = ("Render folder", snd <$> cases)
    expectRstFolder = mkExpectFolder expectRstIndex (expectRst True)
    expectMarkdownFolder = mkExpectFolder expectMarkdownIndex expectMarkdown
    mkExpectFolder expectIndex expectMods =
      ( expectIndex
      , Map.fromList $
          zipWith
            (\(_, mod) expect -> (md_name mod, expect))
            cases
            expectMods
      )

ctx0 :: Context
ctx0 = Context []

cases :: [(String, ModuleDoc)]
cases = [ ("Empty module",
           ModuleDoc Nothing "Empty" Nothing [] [] [] [] [] [])
        , ("Type def with argument",
           ModuleDoc (Just "module-typedef") "Typedef" Nothing [] []
            [TypeSynDoc (Just "type-typedef-t") "T" (Just "T descr") ["a"] (TypeApp Nothing "TT" [TypeApp Nothing "TTT" []]) Nothing]
            [] [] []
          )
        , ("Two types",
           ModuleDoc (Just "module-twotypes") "TwoTypes" Nothing [] []
            [ TypeSynDoc (Just "type-twotypes-t") "T" (Just "T descr") ["a"] (TypeApp Nothing "TT" []) Nothing
            , ADTDoc (Just "data-twotypes-d") "D" Nothing ["d"] [PrefixC (Just "constr-twotypes-d") "D" (Just "D descr") [TypeApp Nothing "a" []]] Nothing
            ]
            [] [] []
          )
        , ("Documented function",
           ModuleDoc (Just "module-function1") "Function1" Nothing [] [] []
            [FunctionDoc (Just "function-function1-f") "f" ctx0 (TypeApp Nothing "TheType" []) (Just "the doc")] [] []
          )
        , ("Undocumented function",
           ModuleDoc (Just "module-function3") "Function3" Nothing [] [] []
            [FunctionDoc (Just "function-function3-f") "f" ctx0 (TypeApp Nothing "TheType" []) Nothing] [] []
          )
        , ("Module with only a type class",
           ModuleDoc (Just "module-onlyclass") "OnlyClass" Nothing [] [] [] []
            [ClassDoc (Just "class-onlyclass-c") "C" Nothing ctx0 ["a"] [ClassMethodDoc (Just "function-onlyclass-member") "member" False ctx0 ctx0 (TypeApp Nothing "a" []) Nothing] Nothing] [])
        , ("Multiline field description",
           ModuleDoc
             (Just "module-multilinefield")
             "MultiLineField"
             Nothing
             []
             []
             [ADTDoc
                (Just "data-multilinefield-d")
                "D"
                Nothing
                []
                [RecordC (Just "constr-multilinefield-d") "D" Nothing [FieldDoc (Just "function-multilinefield-f") "f" (TypeApp Nothing "T" []) (Just "This is a multiline\nfield description")]]
                Nothing]
             []
             []
             []
          )
        , ("Functions with context",
           ModuleDoc
            (Just "module-functionctx") "FunctionCtx"
            Nothing [] [] []
            [ FunctionDoc (Just "function-g") "g"
                (Context [TypeApp Nothing "Eq" [TypeApp Nothing "t" []]])
                (TypeFun [TypeApp Nothing "t" [], TypeApp Nothing "Bool" []])
                (Just "function with context")
            ] [] []
          )
        ]

expectRst ::
     Bool
      -- ^ If True, we are rendering the modules as a folder structure, so
      -- the header underlines get bumped one level up ('^' => '-' => '=')
  -> [T.Text]
expectRst asFolder =
        [ T.empty
        , mkExpectRst asFolder "module-typedef" "Typedef" "" [] []
            [ ".. _type-typedef-t:"
            , ""
            , "**type** `T <type-typedef-t_>`_ a"
            , "  \\= TT TTT"
            , ""
            , "  T descr"
            ] []
        , mkExpectRst asFolder "module-twotypes" "TwoTypes" "" []
            []
            [ ".. _type-twotypes-t:"
            , ""
            , "**type** `T <type-twotypes-t_>`_ a"
            , "  \\= TT"
            , ""
            , "  T descr"
            , ""
            , ".. _data-twotypes-d:"
            , ""
            , "**data** `D <data-twotypes-d_>`_ d"
            , ""
            , "  .. _constr-twotypes-d:"
            , ""
            , "  `D <constr-twotypes-d_>`_ a"
            , ""
            , "    D descr"
            ]
            []
        , mkExpectRst asFolder "module-function1" "Function1" "" [] [] []
            [ ".. _function-function1-f:"
            , ""
            , "`f <function-function1-f_>`_"
            , "  \\: TheType"
            , ""
            , "  the doc"
            ]
        , mkExpectRst asFolder "module-function3" "Function3" "" [] [] []
            [ ".. _function-function3-f:"
            , ""
            , "`f <function-function3-f_>`_"
            , "  \\: TheType"
            ]
        , mkExpectRst asFolder "module-onlyclass" "OnlyClass" ""
            []
            [ ".. _class-onlyclass-c:"
            , ""
            , "**class** `C <class-onlyclass-c_>`_ a **where**"
            , ""
            , "  .. _function-onlyclass-member:"
            , ""
            , "  `member <function-onlyclass-member_>`_"
            , "    \\: a"
            ]
            []
            []
        , mkExpectRst asFolder "module-multilinefield" "MultiLineField" ""
            []
            []
            [ ".. _data-multilinefield-d:"
            , ""
            , "**data** `D <data-multilinefield-d_>`_"
            , ""
            , "  .. _constr-multilinefield-d:"
            , ""
            , "  `D <constr-multilinefield-d_>`_"
            , ""
            , "    .. list-table::"
            , "       :widths: 15 10 30"
            , "       :header-rows: 1"
            , ""
            , "       * - Field"
            , "         - Type"
            , "         - Description"
            , "       * - f"
            , "         - T"
            , "         - This is a multiline field description"
            ]
            []
        , mkExpectRst asFolder "module-functionctx" "FunctionCtx" "" [] [] []
            [ ".. _function-g:"
            , ""
            , "`g <function-g_>`_"
            , "  \\: Eq t \\=\\> t \\-\\> Bool"
            , ""
            , "  function with context"
            ]
        ]
        <> repeat (error "Missing expectation (Rst)")

mkExpectRst ::
     Bool
      -- ^ If True, we are rendering the modules as a folder structure, so
      -- the header underlines get bumped one level up ('^' => '-' => '=')
  -> T.Text
  -> T.Text
  -> T.Text
  -> [T.Text]
  -> [T.Text]
  -> [T.Text]
  -> [T.Text]
  -> T.Text
mkExpectRst asFolder anchor name descr templates classes adts fcts = T.unlines . concat $
    [ [ ".. _" <> anchor <> ":"
      , ""
      , h1 name
      , ""
      ]
    , if T.null descr then [] else [descr, ""]
    , section "Templates" templates
    , section "Typeclasses" classes
    , section "Data Types" adts
    , section "Functions" fcts
    ]
  where
    section title docs =
        if null docs
            then []
            else
                [ h2 title
                , ""
                , T.unlines docs -- NB T.unlines adds a trailing '\n'
                , ""
                ]
    h1 = headerOf (pick '=' '-')
    h2 = headerOf (pick '-' '^')
    pick x y = if asFolder then x else y
    headerOf c t = t <> "\n" <> T.replicate (T.length t) (T.singleton c)

expectRstIndex :: T.Text
expectRstIndex = T.unlines
  [ ".. toctree::"
  , "   :maxdepth: 3"
  , "   :titlesonly:"
  , ""
  , "   Empty <Empty>"
  , "   Function1 <Function1>"
  , "   Function3 <Function3>"
  , "   FunctionCtx <FunctionCtx>"
  , "   MultiLineField <MultiLineField>"
  , "   OnlyClass <OnlyClass>"
  , "   TwoTypes <TwoTypes>"
  , "   Typedef <Typedef>"
  ]

expectMarkdown :: [T.Text]
expectMarkdown =
        [ T.empty
        , mkExpectMD "module-typedef" "Typedef" "" [] []
            [ "<a name=\"type-typedef-t\"></a>**type** [T](#type-typedef-t) a"
            , ""
            , "> = TT TTT"
            , ">"
            , "> T descr"
            ]
            []
        , mkExpectMD "module-twotypes" "TwoTypes" "" [] []
            [ "<a name=\"type-twotypes-t\"></a>**type** [T](#type-twotypes-t) a"
            , ""
            , "> = TT"
            , ">"
            , "> T descr"
            , ""
            , "<a name=\"data-twotypes-d\"></a>**data** [D](#data-twotypes-d) d"
            , ""
            , "> <a name=\"constr-twotypes-d\"></a>[D](#constr-twotypes-d) a"
            , ">"
            , "> > D descr"
            ]
            []
        , mkExpectMD "module-function1" "Function1" "" [] [] []
            [ "<a name=\"function-function1-f\"></a>[f](#function-function1-f)"
            , ""
            , "> : TheType"
            , ">"
            , "> the doc"
            ]
        , mkExpectMD "module-function3" "Function3" "" [] [] []
            [ "<a name=\"function-function3-f\"></a>[f](#function-function3-f)"
            , ""
            , "> : TheType"
            ]
        , mkExpectMD "module-onlyclass" "OnlyClass" ""
            []
            [ "<a name=\"class-onlyclass-c\"></a>**class** [C](#class-onlyclass-c) a **where**"
            , ""
            , "> <a name=\"function-onlyclass-member\"></a>[member](#function-onlyclass-member)"
            , ">"
            , "> > : a"
            ]
            []
            []
        , mkExpectMD "module-multilinefield" "MultiLineField" ""
            []
            []
            [ "<a name=\"data-multilinefield-d\"></a>**data** [D](#data-multilinefield-d)"
            , ""
            , "> <a name=\"constr-multilinefield-d\"></a>[D](#constr-multilinefield-d)"
            , ">"
            , "> > | Field | Type  | Description |"
            , "> > | :---- | :---- | :---------- |"
            , "> > | f     | T     | This is a multiline field description |"
            ]
            []
        , mkExpectMD "module-functionctx" "FunctionCtx" "" [] [] []
            [ "<a name=\"function-g\"></a>[g](#function-g)"
            , ""
            , "> : Eq t =\\> t -\\> Bool"
            , ">"
            , "> function with context"
            ]
        ]
        <> repeat (error "Missing expectation (Markdown)")

mkExpectMD :: T.Text -> T.Text -> T.Text -> [T.Text] -> [T.Text] -> [T.Text] -> [T.Text] -> T.Text
mkExpectMD anchor name descr templates classes adts fcts
  | null templates && null classes && null adts && null fcts && T.null descr = T.empty
  | otherwise = T.unlines $
  ["# <a name=\"" <> anchor <> "\"></a>" <> name]
  <> concat
  [ if T.null descr
        then [""]
        else ["", descr, ""]
  , if null templates then [] else
      [ "## Templates"
      , "", T.unlines templates
      , ""]
  , if null classes then [] else
      [ "## Typeclasses"
      , "", T.unlines classes
      , ""]
  , if null adts then [] else
      [ "## Data Types"
      , "", T.unlines adts
      , ""]
  , if null fcts then [] else
      [ "## Functions"
      , "", T.unlines fcts
      , ""]
  ]

expectMarkdownIndex :: T.Text
expectMarkdownIndex = T.unlines
  [ "* Empty"
  , "* Function1"
  , "* Function3"
  , "* FunctionCtx"
  , "* MultiLineField"
  , "* OnlyClass"
  , "* TwoTypes"
  , "* Typedef"
  ]

renderTest :: RenderFormat -> AnchorMap -> (String, ModuleDoc) -> T.Text -> Tasty.TestTree
renderTest format externalAnchors (name, input) expected =
  testCase name $ do
  let
    renderer = case format of
                 Rst -> renderPage renderRst externalAnchors . renderModule
                 Markdown -> renderPage renderMd externalAnchors . renderModule
                 Html -> error "HTML testing not supported (use Markdown)"
    output = T.strip $ renderer input
    expect = T.strip expected
  compareRendered output expect

renderFolderTest ::
     RenderFormat
  -> AnchorMap
  -> (String, [ModuleDoc])
  -> (T.Text, Map Modulename T.Text)
  -> Tasty.TestTree
renderFolderTest format externalAnchors (name, input) expected =
  testCaseSteps name $ \step -> do
    let
      modStep modName =
        step ("Checking module '" <> T.unpack (unModulename modName) <> "'")

      unexpectedMod modName output = do
        modStep modName
        T.putStrLn $ T.unlines
          [ "Unexpected module in output:"
          , "Expected: <nothing>"
          , "Actual:"
          , T.pack $ show output ]
        assertFailure "Unexpected module in output."

      missingMod modName expect = do
        modStep modName
        T.putStrLn $ T.unlines
          [ "Expected module missing from output:"
          , "Expected:"
          , T.pack $ show expect
          , "Actual: <nothing>" ]
        assertFailure "Expected module missing from output."

      compareMod modName output expect = do
        modStep modName
        compareRendered output expect

    void $ Map.Merge.mergeA
      (Map.Merge.traverseMissing unexpectedMod)
      (Map.Merge.traverseMissing missingMod)
      (Map.Merge.zipWithAMatched compareMod)
      outputModules
      expectModules

    step "Checking index"
    compareRendered outputIndex expectIndex

  where
    (outputIndex, outputModules) = strip $ renderer input
    (expectIndex, expectModules) = strip expected

    renderer = case format of
      Rst -> renderFolder renderRst externalAnchors "html" . renderMap
      Markdown -> renderFolder renderMd externalAnchors "html" . renderMap
      Html -> error "HTML testing not supported (use Markdown)"

    renderMap mods = Map.fromList
      [ (md_name mod, renderModule mod)
      | mod <- mods
      ]

    strip (index, modMap) =
      (T.strip index, T.strip <$> modMap)

compareRendered :: T.Text -> T.Text -> IO ()
compareRendered output expect = do
  unless (output == expect) $ do
    T.putStrLn $ T.unlines
      [ "Output differs from expectation:"
      , "Expected:"
      , T.pack $ show expect
      , "Actual:"
      , T.pack $ show output ]

    let diffs = [  "`" <> e <> "' /= `" <> o <> "'"
                | (e, o) <- zip (T.lines expect) (T.lines output), e /= o ]

    putStrLn $ show (length diffs) <> " different lines"
    mapM_ T.putStrLn diffs

    assertFailure "Output differs from expectation."
