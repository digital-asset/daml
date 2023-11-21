#!/usr/bin/python3
#
# Trivial helper script to create users / databases for Canton nodes
#

import argparse
import sys

def get_parser():
    parser = argparse.ArgumentParser(description = "Helper utility to setup Canton databases for a set of nodes")
    parser.add_argument("--type", help="Type of database to be setup", choices=["postgres"], default="postgres")
    parser.add_argument("--participants", type=int, help="Number of participant dbs to generate (will create dbs named participantX for 1 to N)", default=0)
    parser.add_argument("--domains", type=int, help="Number of domain dbs to generate (will create dbs named domainX for 1 to N)", default=0)
    parser.add_argument("--sequencers", type=int, help="Number of sequencer dbs to generate (will create dbs named sequencerX for 1 to N", default=0)
    parser.add_argument("--mediators", type=int, help="Number of mediators dbs to generate (will create dbs named mediatorX for 1 to N", default=0)
    parser.add_argument("--user", type=str, help="Database user name. If given, the script will also generate a SQL command to create the user", required=True)
    parser.add_argument("--pwd", type=str, help="Database password")
    parser.add_argument("--drop", help="Drop existing", action="store_true")
    return parser.parse_args()

def do_postgres(args):
    print("""
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE rolname = '%s') THEN
      CREATE ROLE \"%s\" LOGIN PASSWORD '%s';
   END IF;
END
$do$;
""" % (args.user, args.user, args.pwd))
    for num, prefix in [(args.domains, "domain"), (args.participants, "participant"), (args.mediators, "mediator"), (args.sequencers, "sequencer")]:
        for ii in range(1, num + 1):
            dbname = prefix + str(ii)
            if args.drop:
                print("DROP DATABASE IF EXISTS %s;" % (dbname))
            print("CREATE DATABASE %s;" % dbname)
            print("GRANT ALL ON DATABASE %s to \"%s\";" % (dbname, args.user))

if __name__ == "__main__":
    args = get_parser()
    if args.type == "postgres":
        do_postgres(args)
    else:
        raise Exception("Unknown database type %s" % (args.type))




