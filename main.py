#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_environment_canada import SourceEnvironmentCanada

if __name__ == "__main__":
    source = SourceEnvironmentCanada()
    launch(source, sys.argv[1:])
