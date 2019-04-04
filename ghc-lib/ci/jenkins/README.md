# AMI to Build GHC on Jenkins Slave

## Script to create new AMI
Use the shell script [create-jenkins-slave.sh](https://github.com/DACH-NY/da/compare/AMI-ghc-creation). Branch is not merged yet as not aware of future state of AMIs

## Adding newly created AMI to Jenkins Slave
We need to add this template to available ones for Jenkins slave to pick up
in the file [here](da/pipeline/infrastructure/services/gcp_jenkins/casc/ec2.yml). Done in this [pull request](
https://github.com/DACH-NY/da/pull/19767/files) plus follow up PRs


## History for Reference
The AMI used as slave is ami-0dc04b59dd27c88cb, based off of ami-0124b815041324e45.
It was originally created from the above mentioned script that used Centos 7 community AMI as source. When I compiled GHC on that slave, I noticed that it needed packages below. Installed them manually using yum install and then created a new AMI from instance using the UI on EC2.
	yum install git
	yum install https://centos7.iuscommunity.org/ius-release.rpm
	yum install python36u
	yum install ncurses-devel
	yum install gcc-c++

Also, on jenkins slave ssh could not be used for the ghc and ghc-lib repos,
instead we used https and updated the .gitconfig and shell script accordingly

