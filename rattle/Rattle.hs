
module Rattle(rattle, CmdOption(..), cmd, cmd_) where

import Development.Shake.Command

rattle :: IO () -> IO ()
rattle act = act
