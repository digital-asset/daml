-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.Render.Tests(mkTestTree)
  where

import           DA.Daml.Doc.Types
import           DA.Daml.Doc.Render

import           Control.Monad.Except
import qualified Data.Text as T
import qualified Data.Text.IO as T

import qualified Test.Tasty.Extended as Tasty
import           Test.Tasty.HUnit



mkTestTree :: IO Tasty.TestTree
mkTestTree = do
  pure $ Tasty.testGroup "DA.Daml.Doc.Render"
    [ Tasty.testGroup "RST Rendering" $
      zipWith (renderTest Rst) cases expectRst
    , Tasty.testGroup "Markdown Rendering" $
      zipWith (renderTest Markdown) cases expectMarkdown
    ]


cases :: [(String, ModuleDoc)]
cases = [ ("Empty module",
           ModuleDoc Nothing "Empty" Nothing [] [] [] [] [])
        , ("Type def with argument",
           ModuleDoc (Just "module-typedef") "Typedef" Nothing [] []
            [TypeSynDoc (Just "type-typedef-t") "T" (Just "T descr") ["a"] (TypeApp Nothing "TT" [TypeApp Nothing "TTT" []])]
            [] []
          )
        , ("Two types",
           ModuleDoc (Just "module-twotypes") "TwoTypes" Nothing [] []
            [ TypeSynDoc (Just "type-twotypes-t") "T" (Just "T descr") ["a"] (TypeApp Nothing "TT" [])
            , ADTDoc (Just "data-twotypes-d") "D" Nothing ["d"] [PrefixC (Just "constr-twotypes-d") "D" (Just "D descr") [TypeApp Nothing "a" []]]
            ]
            [] []
          )
        , ("Documented function with type",
           ModuleDoc (Just "module-function1") "Function1" Nothing [] [] []
            [FunctionDoc (Just "function-function1-f") "f" Nothing (Just $ TypeApp Nothing "TheType" []) (Just "the doc")] []
          )
        , ("Documented function without type",
           ModuleDoc (Just "module-function2") "Function2" Nothing [] [] []
            [FunctionDoc (Just "function-function2-f") "f" Nothing Nothing (Just "the doc")] []
          )
        , ("Undocumented function with type",
           ModuleDoc (Just "module-function3") "Function3" Nothing [] [] []
            [FunctionDoc (Just "function-function3-f") "f" Nothing (Just $ TypeApp Nothing "TheType" []) Nothing] []
          )
        -- The doc extraction won't generate functions without type nor description
        , ("Module with only a type class",
           ModuleDoc (Just "module-onlyclass") "OnlyClass" Nothing [] [] [] []
            [ClassDoc (Just "class-onlyclass-c") "C" Nothing Nothing ["a"] [FunctionDoc (Just "function-onlyclass-member") "member" Nothing (Just (TypeApp Nothing "a" [])) Nothing]])
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
                [RecordC (Just "constr-multilinefield-d") "D" Nothing [FieldDoc (Just "function-multilinefield-f") "f" (TypeApp Nothing "T" []) (Just "This is a multiline\nfield description")]]]
             []
             []
          )
        ]

expectRst :: [T.Text]
expectRst =
        [ T.empty
        , mkExpectRst "module-typedef" "Typedef" "" [] []
            ["\n.. _type-typedef-t:\n\ntype **T a**\n    = TT TTT\n\n  T descr"] []
        , mkExpectRst "module-twotypes" "TwoTypes" "" []
            []
            ["\n.. _type-twotypes-t:\n\ntype **T a**\n    = TT\n\n  T descr"
            , "\n.. _data-twotypes-d:\n\ndata **D d**\n\n  \n  \n  .. _constr-twotypes-d:\n  \n  **D** a\n  \n  D descr"]
            []
        , mkExpectRst "module-function1" "Function1" "" [] [] [] [ "\n.. _function-function1-f:\n\n**f**\n  : TheType\n\n  the doc\n"]
        , mkExpectRst "module-function2" "Function2" "" [] [] [] [ "\n.. _function-function2-f:\n\n**f**\n  :   the doc\n"]
        , mkExpectRst "module-function3" "Function3" "" [] [] [] [ "\n.. _function-function3-f:\n\n**f**\n  : TheType\n\n"]
        , mkExpectRst "module-onlyclass" "OnlyClass" ""
            []
            [ "\n.. _class-onlyclass-c:"
            , "class **C a** where\n  \n  .. _function-onlyclass-member:\n  \n  **member**\n    : a"
            ]
            []
            []
        , mkExpectRst "module-multilinefield" "MultiLineField" ""
            []
            []
            [ "\n.. _data-multilinefield-d:"
            , "data **D**"
            , T.concat
                  [ "  \n  \n"
                  , "  .. _constr-multilinefield-d:\n  \n"
                  , "  **D**\n  \n  \n"
                  , "  .. list-table::\n"
                  , "     :widths: 15 10 30\n"
                  , "     :header-rows: 1\n  \n"
                  , "     * - Field\n"
                  , "       - Type\n"
                  , "       - Description\n"
                  , "     * - f\n"
                  , "       - T\n"
                  , "       - This is a multiline field description"
                  ]
            ]
            []
        ]
        <> repeat (error "Missing expectation (Rst)")

mkExpectRst :: T.Text -> T.Text -> T.Text -> [T.Text] -> [T.Text] -> [T.Text] -> [T.Text] -> T.Text
mkExpectRst anchor name descr templates classes adts fcts = T.unlines $
  [ ".. _" <> anchor <> ":"
  , ""
  , "Module " <> name
  , "-------" <> T.replicate (T.length name) "-"
  , descr, ""
  ]
  <> concat
     [ if null templates then [] else
         [ "Templates"
         , "^^^^^^^^^"
         , T.unlines templates
         , ""]
     , if null classes then [] else
         [ "Typeclasses"
         , "^^^^^^^^^^^"
         , T.unlines (map (<> "\n") classes)
         ]
     , if null adts then [] else
         [ "Data types"
         , "^^^^^^^^^^"
         , T.unlines (map (<> "\n") adts)
         ]
     , if null fcts then [] else
         [ "Functions"
         , "^^^^^^^^^"
         , T.unlines (map (<> "\n") fcts)
         ]
     ]
  -- NB T.unlines adds a trailing '\n'


expectMarkdown :: [T.Text]
expectMarkdown =
        [ T.empty
        , mkExpectMD "module-typedef" "Typedef" "" [] []
            [ "**type <a name=\"type-typedef-t\"></a>T a**  "
            , "&nbsp; = TT TTT"
            , ""
            , "T descr"
            , ""]
            []
        , mkExpectMD "module-twotypes" "TwoTypes" "" [] []
            [ "**type <a name=\"type-twotypes-t\"></a>T a**  "
            , "&nbsp; = TT"
            , ""
            , "T descr"
            , ""
            , "**data <a name=\"data-twotypes-d\"></a>D d**"
            , ""
            , "* <a name=\"constr-twotypes-d\"></a>D a"
            , "  "
            , "  D descr"
            , "  "
            , ""
            ]
            []
        , mkExpectMD "module-function1" "Function1" "" [] [] []
            [ "<a name=\"function-function1-f\"></a>**f**  "
            , "&nbsp; : TheType"
            , ""
            , "the doc"
            , ""
            ]
        , mkExpectMD "module-function2" "Function2" "" [] [] []
            [ "<a name=\"function-function2-f\"></a>**f**  "
            , ""
            , "the doc"
            , ""
            ]
        , mkExpectMD "module-function3" "Function3" "" [] [] []
            [ "<a name=\"function-function3-f\"></a>**f**  "
            , "&nbsp; : TheType"
            , ""
            ]
        , mkExpectMD "module-onlyclass" "OnlyClass" ""
            []
            [ "### <a name=\"class-onlyclass-c\"></a>Class C"
            , ""
            , "**class C a where**"
            , ""
            , "> <a name=\"function-onlyclass-member\"></a>**member**  "
            , "> &nbsp; : a"
            , "> "
            ]
            []
            []
        , mkExpectMD "module-multilinefield" "MultiLineField" ""
            []
            []
            [ "**data <a name=\"data-multilinefield-d\"></a>D**"
            , ""
            , "* <a name=\"constr-multilinefield-d\"></a>D"
            , "  "
            , "  | Field | Type/Description |"
            , "  | :---- | :----------------"
            , "  | f     | T |"
            , "  |       | This is a multiline field description |"
            ]
            []
        ]
        <> repeat (error "Missing expectation (Markdown)")

mkExpectMD :: T.Text -> T.Text -> T.Text -> [T.Text] -> [T.Text] -> [T.Text] -> [T.Text] -> T.Text
mkExpectMD anchor name descr templates classes adts fcts
  | null templates && null classes && null adts && null fcts && T.null descr = T.empty
  | otherwise = T.unlines $
  ["# <a name=\"" <> anchor <> "\"></a>Module " <> name]
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
      [ "## Data types"
      , "", T.unlines adts
      , ""]
  , if null fcts then [] else
      [ "## Functions"
      , "", T.unlines fcts
      , ""]
  ]

renderTest :: DocFormat -> (String, ModuleDoc) -> T.Text -> Tasty.TestTree
renderTest format (name, input) expected =
  testCase name $ do
  let
    renderer = case format of
                 Json -> error "Json encoder testing not done here"
                 Rst -> renderPage . renderSimpleRst
                 Markdown -> renderPage . renderSimpleMD
                 Html -> error "HTML testing not supported (use Markdown)"
                 Hoogle -> error "Hoogle doc testing not yet supported."
    output = T.strip $ renderer input
    expect = T.strip expected

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
