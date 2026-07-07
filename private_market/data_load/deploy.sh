#!/bin/bash

echo "$(date '+%Y-%m-%d %H:%M:%S') STARTING BUILD PROCESS"

BASE_PATH=$(pwd)
TEMP_DIR="dbx"
ZIP_FILE="dbx.zip"

rm -rf "$TEMP_DIR" "$ZIP_FILE"

mkdir -p "$TEMP_DIR"

echo "$(date) Copy adb delta script"

cp -r $BASE_PATH/sql $BASE_PATH/$TEMP_DIR/sql
cp -r $BASE_PATH/utils $BASE_PATH/$TEMP_DIR/utils
cp -r $BASE_PATH/jobs $BASE_PATH/$TEMP_DIR/jobs

echo "$(date) Copying adb delta script finished"

zip -r $ZIP_FILE $TEMP_DIR

echo "$(date) FINISHED with databricks packaging"
