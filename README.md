# file-dedup-tool

Quickly identify duplicate files across directories, ZIP archives, and TAR files using this lightweight Python 3 tool.

This tool recursively scans given paths and computes SHA-256 hashes of file contents.
The results are stored in an SQLite 3 database, and a report can be generated as an XHTML5 page.
Only modules from the Python 3 standard library are used.

## Installation

Make sure that [pipenv](https://packages.debian.org/bookworm/pipenv) is installed, then run the following command:
```sh
pipenv install -e 'git+https://github.com/passchm/file-dedup-tool.git#egg=file-dedup-tool'
```

## Usage

### Scanning directories and files

Run the following command to scan some paths:
```sh
pipenv run python -m dedup.scan ~/Documents/ ~/Downloads/archive.zip ~/Pictures/
```
Replace `~/Documents/`, `~/Downloads/archive.zip`, and `~/Pictures/` with your desired directories or files.

### Generating an XHTML5 report

Run the following command to generate a report file (`./index.xhtml`) with a tree of scanned directories and files:
```sh
pipenv run python -m dedup.render
```
