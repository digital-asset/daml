
module Rattle(rattle, cmd, cmd_) where

import Development.Shake.Command

rattle :: IO () -> IO ()
rattle act = act
