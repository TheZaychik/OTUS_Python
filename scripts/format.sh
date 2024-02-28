#!/usr/bin/env bash
set -x

autoflake --remove-all-unused-imports --recursive --remove-unused-variables --exclude __init__.py,venv .
black .
isort .