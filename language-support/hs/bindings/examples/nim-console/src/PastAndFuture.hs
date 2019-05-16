module PastAndFuture(PastAndFuture(..)) where

import Stream

data PastAndFuture a = PF { past :: [a], future :: Stream a }
