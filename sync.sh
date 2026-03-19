#!/usr/bin/env bash
# Sync this repo to a Databricks workspace.
# Replace <profile> with your Databricks CLI profile and <workspace-path> with your target path.
#
# Example:
#   databricks sync --profile my-profile . /Workspace/Users/me@example.com/my-folder/solace-example
#
databricks sync --profile <profile> . <workspace-path>
