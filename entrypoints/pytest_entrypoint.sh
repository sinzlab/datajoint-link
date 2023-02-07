#!/bin/sh

pdm sync --dev && pdm run pytest "$@"
