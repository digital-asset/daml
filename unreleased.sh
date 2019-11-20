#!/usr/bin/env sh

git log $1 | awk '/^$/{next} /CHANGELOG_END/{flag=0; next} /CHANGELOG_BEGIN/{flag=1; next} flag'
