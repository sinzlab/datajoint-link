#!/bin/sh

pdm sync --dev --section profiling && pdm run python "$@"
