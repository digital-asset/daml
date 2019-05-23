-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options.Applicative.Extended
    ( module Options.Applicative
    , flagYesNoAuto
    ) where

import Options.Applicative


-- | This constructs flags that can be set to yes, no, or auto to control a boolean value
-- with auto using the default.
flagYesNoAuto :: String -> Bool -> String -> Parser Bool
flagYesNoAuto flagName defaultValue helpText =
    option reader (long flagName <> value defaultValue <> help (helpText <> commonHelp))
  where reader = eitherReader $ \case
            "yes" -> Right True
            "no" -> Right False
            "auto" -> Right defaultValue
            s -> Left ("Expected \"yes\", \"no\" or \"auto\" but got " <> show s)
        commonHelp = " Can be set to \"yes\", \"no\" or \"auto\" to select the default (" <> show defaultValue <> ")"

