-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
module DA.Daml.LF.Mangling (mangleIdentifier, unmangleIdentifier) where

import Data.Bits
import Data.Char
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Internal as T (text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Array as TA
import Data.Word

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
mangleIdentifier :: T.Text -> Either String TL.Text
mangleIdentifier txt = case T.foldl' f (MangledSize 0 0) txt of
    MangledSize 0 _ -> Left "Empty identifier"
    MangledSize chars word16s
      | chars == word16s -> Right $! TL.fromStrict txt
      | otherwise -> Right $! TL.fromStrict $
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

unmangleIdentifier :: T.Text -> Either String T.Text
unmangleIdentifier txt = do
  chs <- goStart (T.unpack txt)
  if null chs
    then Left "Empty identifier"
    else Right (T.pack chs)
  where
    go :: (Char -> Bool) -> (String -> Either String String) -> String -> Either String String
    go isAllowed followUp = \case
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
      ch : chs -> if isAllowed ch
        then (ch :) <$> followUp chs
        else Left ("Unexpected unescaped character " ++ show ch)
    
    goStart = go isAllowedStart goPart
    goPart = go isAllowedPart goPart

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
