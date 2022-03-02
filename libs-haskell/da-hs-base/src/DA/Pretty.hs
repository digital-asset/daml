-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}

-- | This library provides the basis for all our pretty-printing. It extends
-- "Text.PrettyPrint.Annotated.Extended" from 'da-base' with support for
-- syntax-highlighting and further combinators.
--
-- Whenever you need to build pretty-printing support for a domain-specific
-- library it is recommended to build on top of this module.
--
-- 1
module DA.Pretty
  (
    -- * Convenience re-export
    module Text.PrettyPrint.Annotated.Extended

    -- ** List combinators
  , angledList
  , fcommasep

  , Pretty(..)
  , PrettyLevel(..)
  , prettyNormal
  , pretty

    -- * Syntax-highlighting support
  , SyntaxClass(..)

  , annotateSC

  , renderPlain
  , renderPretty
  , renderPlainOneLine
  , renderColored

  -- * Syntax-highlighted html support
  , renderHtmlDocumentText
  , renderHtml
  , highlightStylesheet

  -- ** Annotation combinators
  , operator_
  , error_
  , type_
  , typeDoc_
  , keyword_
  , label_
  ) where


import           Data.String
import qualified Data.Text.Extended          as T

import           Orphans.Lib_pretty ()

import qualified Text.Blaze.Html.Renderer.String as BlazeRenderer
import qualified Text.Blaze.Html5                as H
import qualified Text.Blaze.Html5.Attributes     as A

import           Text.PrettyPrint.Annotated.HughesPJClass hiding ((<>), style, text)
import           Text.PrettyPrint.Annotated.Extended

import           System.Console.ANSI
                 ( SGR(..), ConsoleLayer(..), ColorIntensity(..), Color(..)
                 , setSGRCode
                 )


-- | Classes of syntax elements, which are used for highlighting.
data SyntaxClass
    = OperatorSC
      -- ^ Annotation to use as a no-op for highlighting.
    | KeywordSC
    | PredicateSC
    | ConstructorSC
    | TypeSC
    | ErrorSC
    | WarningSC
    | InfoSC
    | HintSC
    | LinkSC T.Text T.Text
    -- ^ @LinkSC url title@: Create a link to the 'url'
    | IdSC T.Text
    -- ^ @IdSC id@: Identifier for the node. For linking into.
    | OnClickSC T.Text
    deriving (Eq, Ord, Show)


------------------------------------------------------------------------------
-- Types and classes to organize pretty-pringing
------------------------------------------------------------------------------

pretty :: Pretty a => a -> Doc ann
pretty = pPrint

-- internal types
-----------------

-- | Colors used for pretty printing to an ANSII-colored string.
type ColorWithIntensity = (ColorIntensity, Color)

-- | A 'String' annotated with coloring information.
--
-- All of its constructors are lazy as this structure only serves as an
-- intermediate structure between rednering a document and conerting the
-- annotations to a 'String' with ANSI-escape codes for setting colors.
data ColoredString
    = DAMLeaf String
    | CSNode ColorWithIntensity ColoredString
    | CSAppend ColoredString ColoredString
    | CSEmpty
    deriving (Show)


-- instances
------------

instance Semigroup ColoredString where
    (<>) = CSAppend

instance Monoid ColoredString where
    mempty  = CSEmpty

------------------------------------------------------------------------------
-- Rendering
------------------------------------------------------------------------------

-- | Render a colored string to a string in the context of the given starting
-- color, which will be re-established at the very end of the returned string.
coloredStringToString :: Maybe ColorWithIntensity -> ColoredString -> String
coloredStringToString currentColor0 c0 =
    go c0 resetColorIfNecessary (currentColor0, currentColor0)
  where
    resetColorIfNecessary (currentColor, _)
      | currentColor /= currentColor0 = setColor currentColor0
      | otherwise                     = []

    setColor Nothing                   = setSGRCode [Reset]
    setColor (Just (intensity, color)) =
        setSGRCode [SetColor Foreground intensity color]

    -- continuation-passing-based implementation of a pre-order traversal of
    -- the coloring tree with lazy injection of color-change codes. Note that
    -- we take care to only use (++) in a right-associative manner to ensure
    -- linear complexity.
    go :: ColoredString
        -> ((Maybe ColorWithIntensity, Maybe ColorWithIntensity) -> String)
        ->  (Maybe ColorWithIntensity, Maybe ColorWithIntensity)
        -> String
    go c k st@(currentColor, desiredColor) = case c of
        CSEmpty      -> k st
        CSAppend l r -> go l (go r k) st
        DAMLeaf s
          | null s                       -> k st
          | currentColor == desiredColor -> s ++ k st
          | otherwise                    ->
              setColor desiredColor ++ s ++ k (desiredColor, desiredColor)
        CSNode desiredColor' c' ->
          -- build new continuation that calls the old one with the currently
          -- desired color.
          let k' (currentColor', _) = k (currentColor', desiredColor)
          -- flatten the annotated node with the newly desired color.
          in  go c' k' (currentColor, Just desiredColor')


-- | Render a 'Document' as an ANSII colored string.
renderColored :: Doc SyntaxClass -> String
renderColored doc =
      coloredStringToString Nothing
    $ renderMonoidal style handleAnn handleChar handleString doc
    -- renderC reducedColored Nothing Nothing
  where
    style = defaultStyle { lineLength = 100 }
    handleChar c  = DAMLeaf [c]
    handleString  = DAMLeaf
    handleAnn ann = case ann of
      OperatorSC     -> CSNode (Dull,  Red)
      KeywordSC      -> CSNode (Dull,  Green)
      PredicateSC    -> CSNode (Dull,  Magenta)
      ConstructorSC  -> CSNode (Vivid, Blue)
      TypeSC         -> CSNode (Vivid, Green)
      ErrorSC        -> CSNode (Vivid, Red)
      WarningSC      -> CSNode (Vivid, Yellow)
      HintSC         -> CSNode (Vivid, Blue)
      InfoSC         -> CSNode (Vivid, Magenta)
      LinkSC _ _     -> CSNode (Vivid, Green)
      IdSC _         -> id
      OnClickSC _    -> id

-- | Render without any syntax annotations
renderPlain :: IsString string => Doc SyntaxClass -> string
renderPlain = fromString . render

-- | Pretty print and render without any syntax annotations
renderPretty :: (Pretty a, IsString string) => a -> string
renderPretty  = renderPlain . pretty

-- | Render without any syntax annotations and vertical spaces are converted
-- to horizontal spaces.
renderPlainOneLine :: Doc SyntaxClass -> String
renderPlainOneLine = renderStyle (defaultStyle { mode = OneLineMode })

------------------------------------------------------------------------------
-- Render to HTML
------------------------------------------------------------------------------

-- How many chars should be at most in a rendered line?
type LineWidth = Int

-- Use the standard DA colors for highlighting
cssStyle :: H.Html
cssStyle = H.style $ H.text highlightStylesheet

-- Render a whole 'H.Html' document with DA colors for highlighting to 'T.Text'
renderHtmlDocumentText :: LineWidth -> Doc SyntaxClass -> T.Text
renderHtmlDocumentText lineWidth =
    T.pack . BlazeRenderer.renderHtml . renderHtmlDocument lineWidth

-- Render a whole 'H.Html' document with DA colors for highlighting to 'H.Html'
renderHtmlDocument :: LineWidth -> Doc SyntaxClass -> H.Html
renderHtmlDocument lineWidth doc =
    H.docTypeHtml $ H.head cssStyle <> (H.body H.! A.class_ (H.textValue highlightClass) $ renderHtml lineWidth doc)


-- | Render one of our documents to 'H.Html'
renderHtml
    :: LineWidth
    -> Doc SyntaxClass -> H.Html
renderHtml lineWidth =
    renderMonoidal style applySyntaxClass prettyCharToHtml prettyStringToHtml
  where
    style = defaultStyle
        { lineLength = lineWidth
        }

-- | Apply a syntax-class annotation.
applySyntaxClass :: SyntaxClass -> H.Html -> H.Html
applySyntaxClass = \case
    OperatorSC     -> apply "operator"
    KeywordSC      -> apply "keyword"
    PredicateSC    -> apply "predicate"
    ConstructorSC  -> apply "constructor"
    TypeSC         -> apply "type"
    WarningSC      -> apply "warning"
    ErrorSC        -> apply "error"
    HintSC         -> apply "hint"
    InfoSC         -> apply "info"
    IdSC  x        -> H.span H.! A.id (H.preEscapedTextValue x)
    LinkSC url title ->
      H.a H.! A.href (H.preEscapedTextValue url) H.! A.title (H.preEscapedTextValue title)
    OnClickSC x    -> H.span H.! A.class_ "da-hl-link" H.! A.onclick (H.preEscapedTextValue x)
  where
    apply :: H.AttributeValue -> H.Html -> H.Html
    apply class_ =
        H.span H.! A.class_ ("da-hl-" <> class_)

-- | Translate pretty-printed text to Html.
prettyStringToHtml :: String -> H.Html
prettyStringToHtml =
    go []
  where
    go :: String -> String -> H.Html
    go acc cs0 = case cs0 of
        []        -> currentLine
        ('\r':cs) -> linebreak cs
        ('\n':cs) -> linebreak cs
        (c   :cs) -> go (c:acc) cs
      where
        currentLine  =
          if null acc then mempty else H.toHtml (reverse acc)
          --unless_ (null acc) ((H.span H.! A.class_ "da-hl-nobr") $ H.toHtml $ reverse acc)
        linebreak cs = currentLine <> H.br <> go [] cs

-- | Translate pretty-printed 'Char's to 'H.Html'.
prettyCharToHtml :: Char -> H.Html
prettyCharToHtml = prettyStringToHtml . return

------------------------------------------------------------------------------
-- Additional generic combinators
------------------------------------------------------------------------------

annotateSC :: SyntaxClass -> Doc SyntaxClass -> Doc SyntaxClass
annotateSC = annotate

type_ :: String -> Doc SyntaxClass
type_ = typeDoc_ . string

typeDoc_ :: Doc SyntaxClass -> Doc SyntaxClass
typeDoc_ = annotateSC TypeSC

operator_ :: String -> Doc SyntaxClass
operator_ = annotateSC OperatorSC . string

keyword_ :: String -> Doc SyntaxClass
keyword_ = annotateSC KeywordSC . string

error_ :: Doc SyntaxClass -> Doc SyntaxClass
error_ = annotateSC ErrorSC

-- | Pretty print a list of documents with prefixed bracs and commas.
angledList :: [Doc a] -> Doc a
angledList = prefixedList "<" ">" ","

-- | Generic combinator for prefixing a list with delimiters and commas.
prefixedList :: Doc a -> Doc a -> Doc a -> [Doc a] -> Doc a
prefixedList leftParen rightParen separator = \case
    []     -> leftParen <-> rightParen
    (d:ds) -> sep [cat (leftParen <-> d : map (separator <->) ds), rightParen]

-- | Pretty print a list of values as a comma-separated list wrapped in
-- paragraph mode.
fcommasep :: [Doc a] -> Doc a
fcommasep = fsep . punctuate comma

-- | Label a document.
label_ :: String -> Doc a -> Doc a
label_ t d = sep [string t, nest 2 d]


------------------------------------------------------------------------------
--- Embedded stylesheets
------------------------------------------------------------------------------

highlightClass :: T.Text
highlightClass = "da-code"

-- | Highlighting stylesheet for Visual Studio Code.
highlightStylesheet :: T.Text
highlightStylesheet = "\
\." <> highlightClass <> " { \
\  font-family: monospace; line-height: 1.1em; white-space: pre; padding: 10px; \
\  position: absolute; width: 100%; height: 100%; \
\} \
\.da-hl-error { color: var(--vscode-terminal-ansiBrightRed); } \
\.da-hl-warning { color: var(--vscode-terminal-ansiYellow); } \
\.da-hl-operator { color: var(--vscode-terminal-ansiRed); } \
\.da-hl-keyword { color: var(--vscode-terminal-ansiGreen); } \
\.da-hl-type { color: var(--vscode-terminal-ansiBrightYellow); } \
\.da-hl-comment { color: var(--vscode-terminal-ansiWhite); } \
\.da-hl-parens { color: var(--vscode-terminal-ansiYellow); } \
\.da-hl-predicate { color: var(--vscode-terminal-ansiMagenta); } \
\.da-hl-constructor { color: var(--vscode-terminal-ansiBrightBlue); } \
\.da-hl-proof-step { color: var(--vscode-terminal-ansiBlue); } \
\.da-hl-link { color: var(--link-color); text-decoration: underline; cursor: pointer; } \
\.da-hl-nobr { white-space: pre; }"
