-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options.Applicative.Extended
    ( module Options.Applicative
    , YesNoAuto (..)
    , flagYesNoAuto
    , flagYesNoAuto'
    , determineAuto
    ) where

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

-- | This constructs flags that can be set to yes, no, or auto, with auto being the default.
-- This maps yes to "Just true", no to "Just False" and auto to "Nothing"
flagYesNoAuto' :: String -> String -> Mod OptionFields YesNoAuto -> Parser YesNoAuto
flagYesNoAuto' flagName helpText mods =
    option reader (long flagName <> value Auto <> help helpText <> completeWith ["yes", "no", "auto"] <> mods)
  where reader = eitherReader $ \case
            "yes" -> Right Yes
            "no" -> Right No
            "auto" -> Right Auto
            s -> Left ("Expected \"yes\", \"no\" or \"auto\" but got " <> show s)

-- | This constructs flags that can be set to yes, no, or auto to control a boolean value
-- with auto using the default.
flagYesNoAuto :: String -> Bool -> String -> Mod OptionFields YesNoAuto -> Parser Bool
flagYesNoAuto flagName defaultValue helpText mods =
    determineAuto defaultValue <$> flagYesNoAuto' flagName (helpText <> commonHelp) mods
    where
        commonHelp = " Can be set to \"yes\", \"no\" or \"auto\" to select the default (" <> show defaultValue <> ")"
