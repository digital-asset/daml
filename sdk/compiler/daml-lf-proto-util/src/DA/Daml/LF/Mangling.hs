-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf, NondecreasingIndentation #-}
module DA.Daml.LF.Mangling
    ( UnmangledIdentifier(..)
    , mangleIdentifier
    , unmangleIdentifier
    ) where

import Data.Bits
import Data.Char
import Data.Coerce
import Data.Either.Combinators
import qualified Data.Text as T
import qualified Data.Text.Array as TA
import qualified Data.Text.Internal as T (text)
import qualified Data.Text.Read as T
import Data.Word

-- Daml-LF talks about *identifier* to build up different kind of
-- names.
--
-- Daml-LF identifiers are non-empty string of ASCII letters, `$`,
-- and `_` as first character, and ASCII letters, ASCII digits, `$`,
-- and `_` afterwords.
--
-- To mangle an Daml identifier into a Daml-LF identifier, we:
--
-- * Pass through characters Daml-LF allows apart from `$`. Note that
--   the first character is different -- we won't be able to accept
--   digits.
-- * We escape `$` to `$$`.
-- * We escape any other characters to its unicode codepoint with `$`
--   as control character. If the codepoint fits in 4 hex digits, it'll
--   just be `$uABCD`, otherwise `$UABCD1234`, with uppercase `U`. This
--   is what C++, C#, D, and several other languages use, see
--   <https://en.wikipedia.org/wiki/UTF-16>
--
-- Field names, variant constructor names, variable names are simple
-- identifiers. On the other hand module names, type constructor names
-- and, value reference names are non-empty sequences of
-- identifiers. Such sequence of identifier are called *dotted name*
-- in the proto encoding/decoding code.
--
-- IMPORTANT: keep in sync with
-- `com.daml.lf.data.Ref.DottedName.fromSegments`

isAsciiLetter :: Char -> Bool
isAsciiLetter c = isAsciiLower c || isAsciiUpper c

isAllowedStart :: Char -> Bool
isAllowedStart c = c == '_' || isAsciiLetter c

isAllowedPart :: Char -> Bool
isAllowedPart c = c == '_' || isAsciiLetter c || isDigit c

data MangledSize = MangledSize
    { _unmangledChars :: !Int
    , _mangledWord16s :: !Int
    }

ord' :: Char -> Word16
ord' = fromIntegral . ord

-- | We spend a fair amount of time in encodeModule which in turn calls
-- `mangleIdentifier` all over the place so we use a heavily optimized implementation.
-- In particular, we optimize for the case where don’t have to do any mangling and
-- can avoid allocating new `Text`s and we optimize the case where we do have to
-- mangle by preallocating the array of the right size and writing the characters
-- directly to that.
mangleIdentifier :: T.Text -> Either String T.Text
mangleIdentifier txt = case T.foldl' f (MangledSize 0 0) txt of
    MangledSize 0 _ -> Left "Empty identifier"
    MangledSize chars word16s
      | chars == word16s -> Right txt
      | otherwise -> Right $!
        let !arr = TA.run $ do
            a <- TA.new word16s
            let poke !j !minj !x
                  | j < minj = pure ()
                  | otherwise = do
                        let !x' = x `unsafeShiftR` 4
                        let !r = x .&. 0xF
                        TA.unsafeWrite a j (fromIntegral $ ord $ intToDigit r)
                        poke (j - 1) minj x'
                go !i !t = case T.uncons t of
                    Nothing -> pure ()
                    Just (!c, !t')
                      | isAllowedStart c || i > 0 && isDigit c -> TA.unsafeWrite a i (fromIntegral $ ord c) >> go (i + 1) t'
                      | c == '$' -> do
                            TA.unsafeWrite a i (ord' '$')
                            TA.unsafeWrite a (i + 1) (ord' '$')
                            go (i + 2) t'
                      | ord c <= 0xFFFF -> do
                            TA.unsafeWrite a i (ord' '$')
                            TA.unsafeWrite a (i + 1) (ord' 'u')
                            poke (i + 5) (i + 2) (ord c)
                            go (i + 6) t'
                      | otherwise -> do
                            TA.unsafeWrite a i (ord' '$')
                            TA.unsafeWrite a (i + 1) (ord' 'U')
                            poke (i + 9) (i + 2) (ord c)
                            go (i + 10) t'
            go 0 txt
            pure a
        in T.text arr 0 word16s
    where f :: MangledSize -> Char -> MangledSize
          f (MangledSize chars word16s) c
            | isAllowedStart c || chars > 0 && isDigit c = MangledSize (chars + 1) (word16s + 1)
            | c == '$' = MangledSize 1 (word16s + 2)
            | ord c > 0xFFFF = MangledSize 1 (word16s + 10)
            | otherwise = MangledSize 1 (word16s + 6)

-- | Newtype to make it explicit when we have already unmangled a string.
newtype UnmangledIdentifier = UnmangledIdentifier { getUnmangledIdentifier :: T.Text }

unmangleIdentifier :: T.Text -> Either String UnmangledIdentifier
unmangleIdentifier txt = mapLeft (\err -> "Could not unmangle name " ++ show txt ++ ": " ++ err) $ coerce $ do
  case T.uncons txt of
      Nothing -> Left "Empty identifier"
      Just (c, _)
        | isAllowedStart c || c == '$' -> go txt
        | otherwise -> Left ("Invalid start character: " <> show c)
  where
    go :: T.Text -> Either String T.Text
    go s = case T.uncons s of
        Nothing -> Right T.empty
        Just ('$', s) ->
              case T.uncons s of
                  Just ('$', s) -> T.cons '$' <$> go s
                  Just ('u', s) -> do
                      (ch, s') <- readEscaped "$u" 4 s
                      T.cons ch <$> go s'
                  Just ('U', s) -> do
                      (ch, s') <- readEscaped "$U" 8 s
                      T.cons ch <$> go s'
                  _ -> Left "Control character $ should be followed by $, u or U"
        Just (char, _) -> case T.span isAllowedPart s of
              (prefix, suffix)
                  | T.null prefix -> Left ("Unexpected unescaped character " ++ show char)
                  | otherwise -> fmap (prefix <>) (go suffix)

    readEscaped what n chs0 = let
      (escaped, chs) = T.splitAt n chs0
      in if
        | T.length escaped < n ->
            Left ("Expected " ++ show n ++ " characters after " ++ what ++ ", but got " ++ show (T.length escaped))
        | not (T.all isEscapeSequenceChars escaped) ->
            Left ("Expected only lowercase hex code in escape sequences, but got " ++ show escaped)
        | otherwise ->
          -- We’ve already done the validation so fromRight is safe.
          Right (toEnum (fst $ fromRight (error $ "Internal error in unmangleIdentifier: " <> show escaped) $ T.hexadecimal escaped), chs)

    -- only lowercase, as per printf
    isEscapeSequenceChars c = isDigit c || c >= 'a' && c <= 'f'
