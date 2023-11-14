#!/bin/bash

# clone the repo in the required directory
mkdir -p $GIT_SYNC_ROOT
cd $GIT_SYNC_ROOT
if [ -z "${GIT_SYNC_BRANCH}" ]; then
   git clone $GIT_SYNC_REPO $GIT_SYNC_DIR
else
   git clone $GIT_SYNC_REPO -b $GIT_SYNC_BRANCH $GIT_SYNC_DIR
fi

# check status of git clone command
if [ $? -eq 0 ]; then
  echo "Successfully cloned the repository."
else
  echo "An error occurred while running git clone."
fi