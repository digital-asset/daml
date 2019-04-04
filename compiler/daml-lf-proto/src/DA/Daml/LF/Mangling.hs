-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
module DA.Daml.LF.Mangling (mangleIdentifier, unmangleIdentifier) where

import qualified Data.Set as Set
import qualified Data.Text as T
import Text.Printf (printf)
  
-- DAML-LF talks about *identifier* to build up different kind of
-- names.
--
-- DAML-LF identifiers are non-empty string of ASCII letters, `$`,
-- and `_` as first character, and ASCII letters, ASCII digits, `$`,
-- and `_` afterwords.
--
-- To mangle an DAML identifier into a DAML-LF identifier, we:
--
-- * Pass through characters DAML-LF allows apart from `$`. Note that
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
-- `com.digitalasset.daml.lf.data.Ref.DottedName.fromSegments`

asciiLetter :: Set.Set Char
asciiLetter = Set.fromList ['a'..'z'] <> Set.fromList ['A'..'Z']

asciiDigit :: Set.Set Char
asciiDigit = Set.fromList ['0'..'9']

allowedStart :: Set.Set Char
allowedStart = Set.insert '_' asciiLetter

allowedPart :: Set.Set Char
allowedPart = Set.insert '_' (asciiLetter <> asciiDigit)

mangleIdentifier :: T.Text -> Either String T.Text
mangleIdentifier txt = case T.unpack txt of
  [] -> Left "Empty identifier"
  ch : chs -> Right (T.pack (escapeStart ch ++ concatMap escapePart chs))

escapeStart :: Char -> String
escapeStart ch = if
  | ch == '$' -> "$$"
  | Set.member ch allowedStart -> [ch]
  | otherwise -> escapeChar ch

escapePart :: Char -> String
escapePart ch = if
  | ch == '$' -> "$$"
  | Set.member ch allowedPart -> [ch]
  | otherwise -> escapeChar ch

escapeChar :: Char -> String
escapeChar ch = let 
  codePoint = fromEnum ch
  in if codePoint > 0xFFFF
    then printf "$U%08x" codePoint
    else printf "$u%04x" codePoint

unmangleIdentifier :: T.Text -> Either String T.Text
unmangleIdentifier txt = do
  chs <- goStart (T.unpack txt)
  if null chs
    then Left "Empty identifier"
    else Right (T.pack chs)
  where
    go :: Set.Set Char -> (String -> Either String String) -> String -> Either String String
    go allowedChars followUp = \case
      [] -> Right []
      ['$'] -> Left "Got control character $ with nothing after it. It should be followed by $, u, or U"
      '$' : ctrlCh : chs0 -> case ctrlCh of
        '$' -> ('$' :) <$> followUp chs0
        'u' -> do
          (ch, chs1) <- readEscaped "$u" 4 chs0
          (ch :) <$> followUp chs1
        'U' -> do
          (ch, chs1) <- readEscaped "$U" 8 chs0
          (ch :) <$> followUp chs1
        ch -> Left ("Control character $ should be followed by $, u, or U, but got " ++ show ch)
      ch : chs -> if Set.member ch allowedChars
        then (ch :) <$> followUp chs
        else Left ("Unexpected unescaped character " ++ show ch)
    
    goStart = go allowedStart goPart
    goPart = go allowedPart goPart

    readEscaped what n chs0 = let
      (escaped, chs) = splitAt n chs0
      in if
        | length escaped < n ->
            Left ("Expected " ++ show n ++ " characters after " ++ what ++ ", but got " ++ show (length escaped))
        | not (all (`Set.member` escapeSequencesChars) escaped) ->
            Left ("Expected only lowercase hex code in escape sequences, but got " ++ show escaped)
        | otherwise -> Right (toEnum (read ("0x" ++ escaped)), chs)

    -- only lowercase, as per printf
    escapeSequencesChars = Set.fromList (['a'..'f'] ++ ['0'..'9'])
