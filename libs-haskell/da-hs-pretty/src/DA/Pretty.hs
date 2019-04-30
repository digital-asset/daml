-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

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

    -- ** Bytestring combinators
  , byteStringBase64
  , byteStringUtf8Decoded

    -- ** List combinators
  , numbered
  , numberedNOutOf
  , bracketedList
  , bracketedSemicolonList
  , bracedList, vbracedList
  , angledList, vangledList
  , parallelBracedList
  , fcommasep

  , PrettyLevel(..)
  , Pretty(..)
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
  , renderHtmlDocument

  , renderHtmlDocumentWithStyleText
  , renderHtmlDocumentWithStyle

  , renderHtml

  , highlightStylesheet
  , highlightClass

  -- ** Annotation combinators
  , comment_
  , predicate_
  , operator_
  , error_
  , warning_
  , type_
  , typeDoc_
  , keyword_
  , label_
  , paren_

    -- ** Comment combinators
  , lineComment
  , lineComment_
  , multiComment

    -- Utilities
  , prettyText
  , toPrettyprinter
  ) where


import qualified Data.ByteString.Base64.URL  as Base64.Url
import qualified Data.ByteString.Char8       as BC8
import           Data.String
import qualified Data.Text.Extended          as T
import qualified Data.Text.Encoding          as TE

import           DA.Prelude

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

import Data.Text.Prettyprint.Doc.Syntax (SyntaxClass(..))
import qualified Data.Text.Prettyprint.Doc.Syntax as Prettyprinter

-- | Adapter to ease migration to prettyprinter. This drops all annotations.
toPrettyprinter :: Pretty a => a -> Prettyprinter.Doc ann
toPrettyprinter a =
    Prettyprinter.concatWith (\x y -> x <> Prettyprinter.hardline <> y) $
    fmap Prettyprinter.pretty $ T.splitOn "\n" (renderPlain $ pretty a :: T.Text)

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
      ParensSC       -> CSNode (Dull,  Yellow)
      CommentSC      -> CSNode (Dull,  White)
      PredicateSC    -> CSNode (Dull,  Magenta)
      ConstructorSC  -> CSNode (Vivid, Blue)
      ProofStepSC    -> CSNode (Dull,  Blue)
      TypeSC         -> CSNode (Vivid, Green)
      ErrorSC        -> CSNode (Vivid, Red)
      WarningSC      -> CSNode (Vivid, Yellow)
      HintSC         -> CSNode (Vivid, Blue)
      InfoSC         -> CSNode (Vivid, Magenta)
      LinkSC _ _     -> CSNode (Vivid, Green)
      NoAnnotationSC -> id
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

-- What style should be used for the body?
type BodyStyle = T.Text

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


-- Render a whole 'H.Html' document with DA colors for highlighting to 'T.Text'
renderHtmlDocumentWithStyleText :: LineWidth -> BodyStyle -> Doc SyntaxClass -> T.Text
renderHtmlDocumentWithStyleText lineWidth style =
    T.pack . BlazeRenderer.renderHtml . renderHtmlDocumentWithStyle lineWidth style

renderHtmlDocumentWithStyle :: LineWidth -> BodyStyle -> Doc SyntaxClass -> H.Html
renderHtmlDocumentWithStyle lineWidth style doc =
    H.docTypeHtml $ H.head cssStyle
      <> (H.body H.! A.class_ (H.textValue highlightClass) H.! A.style (H.textValue style))
         (renderHtml lineWidth doc)

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
    NoAnnotationSC -> id
    OperatorSC     -> apply "operator"
    KeywordSC      -> apply "keyword"
    ParensSC       -> apply "parens"
    PredicateSC    -> apply "predicate"
    ConstructorSC  -> apply "constructor"
    ProofStepSC    -> apply "proof-step"
    CommentSC      -> apply "comment"
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

-- | The default 'B.ByteString' encoding, which is guaranteed to be parseable.
byteStringBase64 :: BC8.ByteString -> Doc a
byteStringBase64 bs = string $ "0b64." ++ (BC8.unpack $ Base64.Url.encode bs)

-- | A more reable 'B.ByteString' pretty-printing, which tries to perform
-- UTF-8 decoding and falls back to 'byteStringBase64' if that fails.
byteStringUtf8Decoded :: BC8.ByteString -> Doc a
byteStringUtf8Decoded bs =
    case TE.decodeUtf8' bs of
      Left _unicodeExc -> byteStringBase64 bs
      Right t          -> fsep $ map text $ T.lines t

annotateSC :: SyntaxClass -> Doc SyntaxClass -> Doc SyntaxClass
annotateSC = annotate

comment_ :: Doc SyntaxClass -> Doc SyntaxClass
comment_ = annotateSC CommentSC

type_ :: String -> Doc SyntaxClass
type_ = typeDoc_ . string

typeDoc_ :: Doc SyntaxClass -> Doc SyntaxClass
typeDoc_ = annotateSC TypeSC

operator_ :: String -> Doc SyntaxClass
operator_ = annotateSC OperatorSC . string

predicate_ :: String -> Doc SyntaxClass
predicate_ = annotateSC PredicateSC . string

keyword_ :: String -> Doc SyntaxClass
keyword_ = annotateSC KeywordSC . string

paren_ :: Doc SyntaxClass -> Doc SyntaxClass
paren_ = annotateSC ParensSC

error_ :: Doc SyntaxClass -> Doc SyntaxClass
error_ = annotateSC ErrorSC

warning_ :: Doc SyntaxClass -> Doc SyntaxClass
warning_ = annotateSC WarningSC

-- | Pretty print a list of documents with prefixed bracs and commas.
bracedList :: [Doc a] -> Doc a
bracedList = prefixedList lbrace rbrace ","

-- | Pretty print a list of documents with prefixed bracs and commas.
angledList :: [Doc a] -> Doc a
angledList = prefixedList "<" ">" ","

-- | Pretty print a list of documents with prefixed '{|' braces and commas.
parallelBracedList :: [Doc a] -> Doc a
parallelBracedList = prefixedList "{|" "|}" " |"

bracketedList :: [Doc a] -> Doc a
bracketedList = prefixedList lbrack rbrack ","

bracketedSemicolonList :: [Doc a] -> Doc a
bracketedSemicolonList = prefixedList lbrack rbrack ";"

-- | Generic combinator for prefixing a list with delimiters and commas.
prefixedList :: Doc a -> Doc a -> Doc a -> [Doc a] -> Doc a
prefixedList leftParen rightParen separator = \case
    []     -> leftParen <-> rightParen
    (d:ds) -> sep [cat (leftParen <-> d : map (separator <->) ds), rightParen]

-- | A 'braced-list whose elements are vertically concatenated with empty lines.
vbracedList :: [Doc a] -> Doc a
vbracedList []     = string "{ }"
vbracedList (d:ds) = vsep (lbrace <-> d : map (comma <->) ds) $-$ rbrace

-- | An 'angled-list whose elements are vertically concatenated with empty lines.
vangledList :: [Doc a] -> Doc a
vangledList []     = string "< >"
vangledList (d:ds) = vsep ("<" <-> d : map (comma <->) ds) $-$ ">"

-- | Pretty print a list of values as a comma-separated list wrapped in
-- paragraph mode.
fcommasep :: [Doc a] -> Doc a
fcommasep = fsep . punctuate comma

-- | Pretty-print a list of documents with prefixed numbers.
numbered :: [Doc a] -> Doc a
numbered docs =
    vcat $ zipWith pp [(1::Int)..] docs
  where
    n            = length docs
    nString      = show n
    padding      = length nString
    pad cs       = replicate (max 0 (padding - length cs)) ' ' <> cs
    showNumber i = pad (show i) <> "."
    indent       = length (showNumber n) + 1
    pp i doc     = string (showNumber i) $$ nest indent doc

-- | Pretty-print a list of documents with prefixed numbers in the style of
-- @i/n@.
numberedNOutOf :: [Doc a] -> Doc a
numberedNOutOf docs =
    vcat $ zipWith pp [(1::Int)..] docs
  where
    n            = length docs
    nString      = show n
    padding      = length nString
    pad cs       = replicate (max 0 (padding - length cs)) ' ' <> cs
    showNumber i = pad (show i) <> "/" <> nString
    indent       = length (showNumber n) + 1
    pp i doc     = string (showNumber i) $-$ nest indent doc

-- | Label a document.
label_ :: String -> Doc a -> Doc a
label_ t d = sep [string t, nest 2 d]


------------------------------------------------------------------------------
-- Comments
------------------------------------------------------------------------------

lineComment :: Doc SyntaxClass -> Doc SyntaxClass
lineComment d = comment_ $ string "//" <-> d

lineComment_ :: String -> Doc SyntaxClass
lineComment_ = lineComment . string

multiComment :: Doc SyntaxClass -> Doc SyntaxClass
multiComment d = comment_ $ fsep [string "/*", d, string "*/"]

prettyText :: Pretty a => a -> T.Text
prettyText = T.pack . renderPlain . pretty

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

