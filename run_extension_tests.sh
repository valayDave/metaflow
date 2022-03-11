#!/bin/bash

set -o errexit -o nounset -o pipefail

install_deps() {
    for version in 2 3; do
        python$version -m pip install .
    done
}

install_extensions() {
    cd test/extensions
    sh install_packages.sh
}

run_tests() {
    cd test/core && PYTHONPATH=$(pwd)/../../ python3 run_tests.py --num-parallel 8 --tests CardExtensionsImportTest --contexts python3-all-local
}

install_deps && install_extensions && run_tests

