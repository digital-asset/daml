-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options.Applicative.Extended
    ( YesNoAuto (..)
    , flagYesNoAuto
    , flagYesNoAuto'
    , determineAuto
    , determineAutoM
    , optionOnce
    , optionOnce'
    , strOptionOnce
    , lastOr
    ) where

import Data.List.NonEmpty qualified as NE
import GHC.Exts (IsString (..))
import Options.Applicative

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

-- | This constructs flags that can be set to yes, no, or auto, with auto being the default.
-- This maps yes to "Just true", no to "Just False" and auto to "Nothing"
flagYesNoAuto' :: String -> String -> Mod OptionFields YesNoAuto -> Parser YesNoAuto
flagYesNoAuto' flagName helpText mods =
    optionOnce reader (long flagName <> value Auto <> help helpText <> completeWith ["true", "false", "yes", "no", "auto"] <> mods)
  where reader = eitherReader $ \case
            "yes" -> Right Yes
            "true" -> Right Yes
            "no" -> Right No
            "false" -> Right No
            "auto" -> Right Auto
            s -> Left ("Expected \"yes\", \"true\", \"no\", \"false\", or \"auto\" but got " <> show s)

-- | This constructs flags that can be set to yes, no, or auto to control a boolean value
-- with auto using the default.
flagYesNoAuto :: String -> Bool -> String -> Mod OptionFields YesNoAuto -> Parser Bool
flagYesNoAuto flagName defaultValue helpText mods =
    determineAuto defaultValue <$> flagYesNoAuto' flagName (helpText <> commonHelp) mods
    where
        commonHelp = " Can be set to \"yes\", \"no\" or \"auto\" to select the default (" <> show defaultValue <> ")"

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
