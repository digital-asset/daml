#!/usr/bin/env runpipenv
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import os
import xml.etree.ElementTree as ET
import sys
import csv
import re
import argparse
import os.path

NO_LICENSE = 'No license information available'
STRIP_PATTERN = ' \t\n\r'
LICENSES_WHITE_LIST_FILE = 'LICENSES_WHITE_LIST.csv'
PACKAGES_WHITE_LIST_FILE = 'PACKAGES_WHITE_LIST.csv'
WHITE_LISTED_LICENSES = []
WHITE_LISTED_PACKAGES = []


def load_white_listed_licenses(white_listed_licenses_file):
    script_dir = os.path.dirname(__file__)
    print("Loading %s" % white_listed_licenses_file)
    white_listed_licenses = []
    with open(os.path.join(script_dir, white_listed_licenses_file)) as csvfile:
        oss_white_lists = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in oss_white_lists:
            white_listed_licenses.append(
                {'license detail': row['license detail'],
                 'license group': row['license group'],
                 'info': row['info']})
    print("White listed licenses: %s" % white_listed_licenses)
    return white_listed_licenses


def load_white_listed_packages(white_listed_packages_file):
    script_dir = os.path.dirname(__file__)
    print("Loading %s" % white_listed_packages_file)
    white_listed_packages = []
    with open(os.path.join(script_dir, white_listed_packages_file)) as csvfile:
        packages_white_lists = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in packages_white_lists:
            white_listed_packages.append(
                {'language': row['language'],
                 'package': row['package'],
                 'info': row['info']})
    print("White listed packages: %s" % white_listed_packages)
    return white_listed_packages


def enrich_dependency_with_compliance(dependency):
    compliance = is_dependency_compliant(dependency, WHITE_LISTED_LICENSES, WHITE_LISTED_PACKAGES)
    dependency.update(
        {'is_compliant': compliance['is_compliant'],
         'additional_info': compliance['additional_info']})
    return dependency


def retrieve_java_dependency_licenses(dependency):
    licenses = []
    for license in dependency.find('licenses').findall('license'):
        license_name = license.find('name').text
        license_url = license.find('url').text if license.find('url') is not None else 'Not Available'
        licenses.append({'license_name': license_name, 'license_url': license_url})
    if not licenses:
        licenses.append({'license_name': NO_LICENSE, 'license_url': ''})
    return licenses


def retrieve_java_dependencies(maven_file):
    root = ET.parse(maven_file).getroot()
    dependencies = []
    for dependency in root.iter('dependency'):
        group_id = dependency.find('groupId').text
        artifact_id = dependency.find('artifactId').text
        version = dependency.find('version').text
        java_dependency = {
            'language': 'java',
            'package': group_id,
            'artifact': artifact_id,
            'version': version,
            'licenses': retrieve_java_dependency_licenses(dependency),
        }
        dependencies.append(enrich_dependency_with_compliance(java_dependency))
    return dependencies


def read_csv_file(filename):
    with open(filename, 'rb') as csvfile:
        print(csvfile.name)

        reader = csv.reader(csvfile, quoting=csv.QUOTE_MINIMAL, strict=True, delimiter=",")
        reader.next()
        csv_list = list(reader)
        dependencies = []
        for row in csv_list:
            dependency = convert_row(row)
            dependencies.append(enrich_dependency_with_compliance(dependency))
        return dependencies


def convert_row(row):
    name = row[2]
    license_match = re.search('["]?(.*)[(](.*)[)]', row[1])

    if license_match:
        license_url = license_match.group(2)
        license_name = license_match.group(1).strip()

    name_match = re.search('(.*) # (.*) # (.*)', name)

    if name_match:
        name_path = name_match.group(1).strip()
        name_lib = name_match.group(2).strip()
        name_version = name_match.group(3).strip()
    else:
        # print "ERROR: Could not parse library name %s" % name
        sys.exit(1)
    return {
        'category': row[0],
        'language': "Java",
        'licenses': [{'license_name': license_name, 'license_url': license_url}],
        'artifact': name_lib,
        'package': name_path,
        'version': name_version
    }


def save_dependencies_as_csv(dependencies, output_file):
    csv_file = open(output_file, 'w')
    csv_file.write("compliant,language,package,artifact,version,license_name,license_url\n")
    for dependency in dependencies:
        for license in dependency['licenses']:
            csv_line = "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n" % (
                dependency['is_compliant'],
                dependency['language'],
                dependency['package'],
                dependency['artifact'],
                dependency['version'],
                license['license_name'],
                license['license_url'],
                dependency['additional_info'])
            csv_file.write(csv_line)
    csv_file.close()


def is_package_white_listed(package, white_listed_packages):
    for white_listed_package in white_listed_packages:
        if white_listed_package['package'].strip(STRIP_PATTERN) in package.strip(STRIP_PATTERN):
            return {'is_white_listed': True,
                    'additional_info': white_listed_package['info'],
                    'type': 'package'}
    return {'is_white_listed': False}


def is_license_white_listed(licenses, white_listed_licenses):
    for license in licenses:
        for white_listed_license in white_listed_licenses:
            if white_listed_license['license detail'].strip(STRIP_PATTERN).lower() == license['license_name'].strip(
                    STRIP_PATTERN).lower():
                return {'is_white_listed': True,
                        'additional_info': white_listed_license['info'],
                        'type': 'license'}
    return {'is_white_listed': False}


def is_dependency_compliant(dependency, white_listed_licenses, white_listed_packages):
    license = is_license_white_listed(dependency['licenses'], white_listed_licenses)
    package = is_package_white_listed(dependency['package'], white_listed_packages)
    if license['is_white_listed']:
        return {'is_compliant': True, 'additional_info': license['additional_info']}
    elif package['is_white_listed']:
        return {'is_compliant': True, 'additional_info': package['additional_info']}
    else:
        return {'is_compliant': False, 'additional_info': 'not found'}


def retrieve_all_dependencies(java_dependencies_file, java_dependencies_format):
    print('Parsing java dependencies file: %s:' % java_dependencies_file)
    java_dependencies = {}
    if java_dependencies_format == "XML":
        java_dependencies = retrieve_java_dependencies(java_dependencies_file)
    if java_dependencies_format == "CSV":
        java_dependencies = read_csv_file(java_dependencies_file)
    return java_dependencies


def should_fail_build(dependencies):
    should_fail = False
    print("Analyzing the list of dependencies  ...\n")
    for dependency in dependencies:
        if not dependency['is_compliant']:
            print("Dependency %s:%s:%s is not compliant " % (
                dependency['language'], dependency['package'], dependency['artifact']))
            should_fail = True
    return should_fail


def args_parsing():
    parser = argparse.ArgumentParser(description='Validate the license for 3rd party libraries')
    parser.add_argument('--java-deps', dest='java_dependencies_file', required=True,
                        help='generated by mvn license plugin')
    parser.add_argument('--fail-build', dest='fail_build', required=False, default='yes',
                        help='fail build if no compliant dependencies are found (yes / no, default is yes)')
    parser.add_argument('--java-deps-format', dest='java_dependencies_format', required=True,
                        help='format of dependencies file (CSV or XML)')

    return parser.parse_args(sys.argv[1:])


def main(inner_args):
    all_dependencies = retrieve_all_dependencies(inner_args.java_dependencies_file, inner_args.java_dependencies_format)
    all_dependencies_file = 'oss_license_compliance_report_%s.csv' % inner_args.java_dependencies_file.replace('/','-').replace('.','')
    print('Saving all dependencies as %s' % all_dependencies_file)
    save_dependencies_as_csv(all_dependencies, "cache/" + all_dependencies_file)
    if should_fail_build(all_dependencies):
        print("\nAt least one of the dependencies is not compliant - please see %s for more info" % all_dependencies_file)
        print("\nYou can filter the dependencies by the 'Compliant' column")
        if inner_args.fail_build == 'yes':
            print("--fail-build set to 'yes', returning an error code")
            sys.exit(1)


print('*** OSS Licenses Check')
args = args_parsing()
WHITE_LISTED_LICENSES = load_white_listed_licenses(LICENSES_WHITE_LIST_FILE)
WHITE_LISTED_PACKAGES = load_white_listed_packages(PACKAGES_WHITE_LIST_FILE)
main(args)
