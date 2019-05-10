#!/usr/bin/env python3
"""Script to do a new release of the library automatically.

A release consists of a few steps:
- bump the version number
- commit the version bump and make a git tag
- push
- run ./gradlew publish
- bump the version number to a -pre version
- commit the version bump
- push

This script automates these steps so this is a bit less annoying.
"""
import argparse
import os
import re
import subprocess
import textwrap
from typing import Generator, List

VERSION_BUMP_COMMIT_TEMPLATE = textwrap.dedent("""\
    Auto-bump version to %(version)s

    Test plan:
    - fingers crossed

    Auditors: %(auditors)s
""")


class VersionParsingError(Exception):
    pass


class InvalidVersionError(Exception):
    pass


VERSION_RE = re.compile(r'^version = "([^"]+)"', re.MULTILINE)


def run_command(command: List[str], dry_run: bool):
    """Run a command, or print its output in dry-run mode.

    The working directory will be this file's directory, and we'll throw on
    nonzero return status.
    """
    if dry_run:
        print(f'Would run: {command}')
    else:
        subprocess.run(command, check=True, cwd=os.path.dirname(__file__))


def get_version_from_build(build_filename: str) -> str:
    """Parse a build.gradle.kts to get its version."""
    with open(build_filename) as f:
        lines = f.readlines()

    for line in lines:
        match = VERSION_RE.search(line)
        if match:
            return match.group(1)

    raise VersionParsingError(
        f'Could not find a version in {build_filename}')


def build_filenames() -> Generator[str, None, None]:
    """Find all build.gradle.kts files in this directory."""
    for root, _, files in os.walk('.'):
        for file in files:
            if file == 'build.gradle.kts':
                yield os.path.join(root, file)


def version_ints(version: str) -> List[int]:
    """Get the major, minor, patch versions in a list.

    This is needed for comparing versions; versions like 0.1.10 don't work
    with lexicographic comparison.
    """
    return [int(v) for v in re.sub(r'-pre.*', '', version).split('.')]


def set_version(version: str, dry_run: bool):
    """Replace version in all build.gradle.kts files with the current one.

    Raise if there are different versions found, if we can't find any
    versions, or if the requested version is not an increase.
    """
    versions = []
    for build_filename in build_filenames():
        versions.append(get_version_from_build(build_filename))

    if len(set(versions)) != 1:
        raise InvalidVersionError(
            f'Found inconsistent versions in build files: {versions}')

    old_version = versions[0]
    if '-pre' in old_version:
        version_ok = (
            version_ints(old_version) <= version_ints(version))
    else:
        version_ok = version_ints(old_version) < version_ints(version)

    if not version_ok:
        raise InvalidVersionError(
            f'Changing versions from {old_version} to {version} not allowed')

    for build_filename in build_filenames():
        with open(build_filename) as f:
            contents = f.read()
        contents = VERSION_RE.sub(f'version = "{version}"', contents)

        if dry_run:
            print(f'Would set contents of {build_filename} to\n{contents}')
        else:
            with open(build_filename, 'w') as f:
                f.write(contents)


def commit_version_bump(version: str, auditors: str, dry_run: bool):
    """Add build files to the index and commit using a standard message."""
    for build_filename in build_filenames():
        git_command = ['git', 'add', build_filename]
        run_command(git_command, dry_run)

    commit_command = [
        'git', 'commit',
        '-m', VERSION_BUMP_COMMIT_TEMPLATE % {
            'version': version,
            'auditors': auditors,
        }
    ]

    run_command(commit_command, dry_run)


def make_tag(version: str, dry_run: bool):
    """Create a git tag for the specified version."""
    run_command(['git', 'tag', f'v{version}'], dry_run)


def git_push(dry_run: bool):
    """Push (to whatever upstream is), and push tags too."""
    run_command(['git', 'push'], dry_run)
    run_command(['git', 'push', '--tags'], dry_run)


def do_publish(dry_run: bool):
    """Run the gradle command to publish to our maven repo."""
    run_command(['./gradlew', 'publish'], dry_run)


def do_release(version: str, auditors: str, dry_run: bool):
    """Do the full release process."""
    set_version(version, dry_run)
    commit_version_bump(version, auditors, dry_run)
    make_tag(version, dry_run)
    git_push(dry_run)
    do_publish(dry_run)

    version_i = version_ints(version)
    version_i[-1] += 1
    next_version = '.'.join([str(v) for v in version_i])
    next_version += '-pre1'
    set_version(next_version, dry_run)
    commit_version_bump(next_version, auditors, dry_run)
    git_push(dry_run)


def main():
    parser = argparse.ArgumentParser(
        description='Do a release of this library'
    )

    parser.add_argument(
        'version',
        help='The version to release, e.g. 0.1.2'
    )

    parser.add_argument(
        '--auditors',
        '-a',
        help=('The auditors for the version bump commit, exactly as you '
              'would write them in the commit message for phabricator. '
              '(You may need to quote them for the shell.)'),
        required=True
    )

    parser.add_argument(
        '--dry-run',
        '-n',
        help=('Whether to just print the commands instead of running them.'),
        action='store_true'
    )

    args = parser.parse_args()

    do_release(args.version, args.auditors, args.dry_run)


if __name__ == '__main__':
    main()
