-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | A version of "Text.PrettyPrinting.Annotated" with combinators that
-- interact well with our standard Prelude; i.e., support for 'T.Text' values
-- and 'Monoid's.
--
-- NOTE (SM): that the 'Monoid' operator overlap is probably not going to be
-- resolved due to backwards compatibility issues. See:
-- http://www.haskell.org/pipermail/libraries/2011-November/017066.html
--
--
module Text.PrettyPrint.Annotated.Extended
  (
    module Text.PrettyPrint.Annotated.HughesPJ

    -- * Additional or renamed combinators
  , string
  , text
  , shown
  , (<->)
  , ($-$)
  , vsep
  , singleQuotes
  , stringParagraphs

    -- * Additional rendering functions
  , defaultStyle
  , renderMonoidal
  ) where


import Data.Text qualified as T
import Data.List

import Text.PrettyPrint.Annotated.HughesPJ
          hiding ( (<>), (<+>), style, text, ($+$)
                 )
import Text.PrettyPrint.Annotated.HughesPJ qualified as PP

------------------------------------------------------------------------------
-- Additional or renamed combinators
------------------------------------------------------------------------------

-- We use infixr for <-> as the Monoid (<>) operator is also infixr.
-- We use infixl for $-$ as the corresponding $$ operator is also infixl.
infixr 6 <->
infixl 5 $-$

-- | A document of height 1 containing a literal 'String'.
string :: String -> Doc a
string = PP.text

-- | A document of height 1 containing a literal 'T.Text' value.
text :: T.Text -> Doc a
text = string . T.unpack

-- | A document of height 1 containing the 'show'n value as a literal
-- 'String'.
shown :: Show b => b -> Doc a
shown = string . show

-- | Beside, separated by space, unless one of the arguments is 'mempty'.
-- A non-"Elvence.Prelude"-clashing version of '(PP.<+>)'.
(<->) :: Doc a -> Doc a -> Doc a
(<->) = (PP.<+>)

-- | Above, with no overlapping. A synonym for '(PP.$+$)' that is consistent
-- with the naming of '(<->)'.
($-$) :: Doc a -> Doc a -> Doc a
($-$) = (PP.$+$)

-- | The default pretty-printing style.
defaultStyle :: Style
defaultStyle = PP.style

-- | Vertical concatenation of documents interspered with empty lines.
vsep :: [Doc a] -> Doc a
vsep = vcat . intersperse (string "")

-- | Single-quote a document.
singleQuotes :: Doc a -> Doc a
singleQuotes d = char '\'' <> d <> char '\''

stringParagraphs :: String -> Doc a
stringParagraphs = vcat . map (fsep . map string . words) . lines

------------------------------------------------------------------------------
-- Improved rendering
------------------------------------------------------------------------------

-- | Render a document with annotations by translating it's 'Char's and
-- 'String's to a 'Monoid'al result type, which is transformed whenever an
-- annotation is encountered.
renderMonoidal
    :: forall ann r.
       Monoid r
    => Style            -- ^ Pretty printing style to use
    -> (ann -> r -> r)
       -- ^ Apply an annotation to the rendered version of the annotated part
       -- of the document. This function is called exactly once for every
       -- annotation in the document.
    -> (Char -> r)
       -- ^ Render a 'Char'
    -> (String -> r)
       -- ^ Render a 'String', which will include the whitespace from
       -- indentation.
    -> Doc ann -> r
renderMonoidal style applyAnn convChar convText =
    fst . fullRenderAnn
              (mode style)
              (lineLength style)
              (ribbonsPerLine style)
              annPrinter
              (mempty, [])
  where
    annPrinter :: AnnotDetails ann -> (r, [(ann, r)]) -> (r, [(ann, r)])
    annPrinter AnnotStart (currentDoc, stack) =
      case stack of
        (ann, nextDoc) : anns -> (applyAnn ann currentDoc <> nextDoc, anns)
        _                     -> error "renderMonoidal: stack underflow"

    annPrinter (AnnotEnd ann) (currentDoc, stack) =
      (mempty, (ann, currentDoc) : stack)

    annPrinter (NoAnnot td _) (currentDoc, stack) =
      case td of
        Chr  c -> (convChar c <> currentDoc, stack)
        Str  s -> (convText s <> currentDoc, stack)
        PStr s -> (convText s <> currentDoc, stack)
