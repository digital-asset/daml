LINUX=linux
OSX=osx
ifeq ($(shell uname -s), Darwin)
	OS=$(OSX)
else
	OS=$(LINUX)
endif
################################################################################
########################    General Tasks     ##################################
################################################################################
.PHONY: oss-compliance

oss-compliance:
	sh check_oss_license.sh
