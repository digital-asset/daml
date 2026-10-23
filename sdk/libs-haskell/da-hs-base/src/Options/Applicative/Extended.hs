-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options.Applicative.Extended
    ( YesNoAuto (..)
    , flagYesNoAuto
    , flagYesNoAutoFlagResult
    , flagYesNoAutoNoDefault
    , determineAuto
    , determineAutoM
    , optionOnce
    , optionOnce'
    , strOptionOnce
    , optionLast
    , lastOr
    , OverwritableFlagResult (..)
    , getFlagResult
    , optionFlagResult
    , optionOnceFlagResult
    , switchFlagResult
    ) where

import GHC.Exts (IsString (..))
import Options.Applicative
import qualified Options.Applicative.Builder.Internal as OptParseInternal
import Safe (lastMay)

import qualified Data.List.NonEmpty as NE

-- | A morally boolean value with a default third option (called Auto) to be determined later.
data YesNoAuto
    = No
    | Auto
    | Yes
    deriving (Eq, Ord, Show)

-- | Convert YesNoAuto value to Bool by specifying a default value for Auto.
determineAuto :: Bool -> YesNoAuto -> Bool
determineAuto b = \case
    No -> False
    Auto -> b
    Yes -> True

-- | Convert YesNoAuto value to Bool by specifying how default value for Auto should be determined, in a monad.
determineAutoM :: Monad m => m Bool -> YesNoAuto -> m Bool
determineAutoM m = \case
    No -> pure False
    Auto -> m
    Yes -> pure True

-- | This constructs flags that can be set to yes, no, or auto, with no default
-- This maps yes to "Just true", no to "Just False" and auto to "Nothing"
-- Use this when putting this flag behind a combinator like `many`
flagYesNoAutoNoDefault :: String -> String -> Mod OptionFields YesNoAuto -> Parser YesNoAuto
flagYesNoAutoNoDefault flagName helpText mods =
    optionOnce reader (long flagName <> help helpText <> completeWith ["true", "false", "yes", "no", "auto"] <> mods)
  where reader = eitherReader $ \case
            "yes" -> Right Yes
            "true" -> Right Yes
            "no" -> Right No
            "false" -> Right No
            "auto" -> Right Auto
            s -> Left ("Expected \"yes\", \"true\", \"no\", \"false\", or \"auto\" but got " <> show s)

-- | See flagYesNoRaw, this function resolves the YesNoAuto to its real boolean value
flagYesNoAuto :: String -> Bool -> String -> Mod OptionFields YesNoAuto -> Parser Bool
flagYesNoAuto flagName defaultValue helpText mods =
    determineAuto defaultValue <$> flagYesNoRaw flagName defaultValue helpText mods

-- | This constructs flags that can be set to yes, no, or auto to control a boolean value
-- with auto using the default.
flagYesNoRaw :: String -> Bool -> String -> Mod OptionFields YesNoAuto -> Parser YesNoAuto
flagYesNoRaw flagName defaultValue helpText mods =
    flagYesNoAutoNoDefault flagName (helpText <> commonHelp) (value Auto <> mods)
    where
        commonHelp = " Can be set to \"yes\", \"no\" or \"auto\" to select the default (" <> show defaultValue <> ")"

flagYesNoAutoFlagResult :: String -> Bool -> String -> Mod OptionFields YesNoAuto -> Parser (OverwritableFlagResult Bool)
flagYesNoAutoFlagResult flagName defaultValue helpText mods =
  fmap (determineAuto defaultValue) <$> optionFlagResult' (flagYesNoRaw flagName defaultValue helpText) mods

-- | optparse-applicative does not provide useful error messages when a valid
-- option is passed more than once https://github.com/pcapriotti/optparse-applicative/issues/395
--
-- This provides better error messages by constructing two parsers for the same
-- option, where the second parser automatically throws an infomative error.
--
-- If the option is specified a second time, the second parser is invoked and
-- triggers an error.
optionOnce :: ReadM a -> Mod OptionFields a -> Parser a
optionOnce = optionOnce' "Option specified more than once."

optionOnce' :: String -> ReadM a -> Mod OptionFields a -> Parser a
optionOnce' errMsg reader options = const <$> actualParser <*> errorIfTwiceParser
    where
    actualParser = option reader options
    errorIfTwiceParser = option (readerError errMsg) (options <> internal <> value (error "optionOnce: should not happen"))

strOptionOnce :: IsString a => Mod OptionFields a -> Parser a
strOptionOnce = optionOnce str

optionLast :: ReadM a -> Mod OptionFields a -> Parser (Maybe a)
optionLast reader options = lastMay <$> many (option reader options)

-- | @'lastOr' def one@ returns the value of the last succesful @one@, if any,
-- otherwise returns @def@.
--
-- /Note/: if @one@ always succeeds, @lastOr def one@ will loop forever.
--
-- /Note/: @lastOr def one@ will never fail, so it cannot be used with @some@
-- or @many@ (or @lastOr@ itself)
lastOr :: Alternative f => b -> f b -> f b
lastOr def one =
      NE.last <$> NE.some1 one
  <|> pure def

data OverwritableFlagResult a = Provided { userValue :: a } | NotProvided { defaultValue :: a }
  deriving (Show, Eq, Functor)

instance Semigroup (OverwritableFlagResult a) where
  -- Only take the first if its the only provided
  Provided x <> NotProvided _ = Provided x
  _ <> y = y

getFlagResult :: OverwritableFlagResult a -> a
getFlagResult (Provided a) = a
getFlagResult (NotProvided def) = def

-- | Variant of `option` that exposes whether the user provided the argument, or whether the default was used
-- Uses OptParse internals to keep interface the same
optionFlagResult :: ReadM a -> Mod OptionFields a -> Parser (OverwritableFlagResult a)
optionFlagResult = optionFlagResult' . option

optionOnceFlagResult :: ReadM a -> Mod OptionFields a -> Parser (OverwritableFlagResult a)
optionOnceFlagResult = optionFlagResult' . optionOnce

optionFlagResult' :: (Mod OptionFields a -> Parser a) -> Mod OptionFields a -> Parser (OverwritableFlagResult a)
optionFlagResult' makeParser (OptParseInternal.Mod f (OptParseInternal.DefaultProp (Just def) show) props) =
  let mods = OptParseInternal.Mod f (OptParseInternal.DefaultProp Nothing show) props
   in maybe (NotProvided def) Provided <$> optional (makeParser mods)
optionFlagResult' makeParser mods = Provided <$> makeParser mods

-- | Variant of `switch` to match the OverwritableFlagResult type of `optionFlagResult`
-- Given switch only allows setting the boolean "one way", this always returns either
-- `Provided True` or `NotProvided False`
switchFlagResult :: Mod FlagFields (OverwritableFlagResult Bool) -> Parser (OverwritableFlagResult Bool)
switchFlagResult = flag (NotProvided False) (Provided True)
