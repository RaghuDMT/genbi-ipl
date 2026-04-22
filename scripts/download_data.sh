
#!/usr/bin/env bash

set -euo pipefail

RAW_DIR="data/raw"
MALE_URL="https://cricsheet.org/downloads/ipl_male_json.zip"
FEMALE_URL="https://cricsheet.org/downloads/wpl_female_json.zip"
MALE_ZIP="${RAW_DIR}/ipl_male_json.zip"
FEMALE_ZIP="${RAW_DIR}/ipl_female_json.zip"
MALE_DIR="${RAW_DIR}/ipl_json_men"
FEMALE_DIR="${RAW_DIR}/wpl_json"

download_file() {
  local url="$1"
  local output="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fL "$url" -o "$output"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$output" "$url"
  else
    echo "Error: curl or wget is required to download files." >&2
    exit 1
  fi
}

extract_zip() {
  local archive="$1"
  local output_dir="$2"

  mkdir -p "$output_dir"

  if command -v unzip >/dev/null 2>&1; then
    unzip -oq "$archive" -d "$output_dir"
  else
    python -c "import zipfile; zipfile.ZipFile(r'$archive').extractall(r'$output_dir')"
  fi
}

count_json_files() {
  local target_dir="$1"
  find "$target_dir" -type f -name "*.json" | wc -l | tr -d ' '
}

mkdir -p "$RAW_DIR"

echo "Downloading IPL male data..."
download_file "$MALE_URL" "$MALE_ZIP"
echo "Downloading IPL female data..."
download_file "$FEMALE_URL" "$FEMALE_ZIP"

echo "Extracting IPL male data to $MALE_DIR..."
extract_zip "$MALE_ZIP" "$MALE_DIR"
echo "Extracting IPL female data to $FEMALE_DIR..."
extract_zip "$FEMALE_ZIP" "$FEMALE_DIR"

MALE_COUNT=$(count_json_files "$MALE_DIR")
FEMALE_COUNT=$(count_json_files "$FEMALE_DIR")
TOTAL_COUNT=$((MALE_COUNT + FEMALE_COUNT))

echo "Male JSON files: $MALE_COUNT"
echo "Female JSON files: $FEMALE_COUNT"
echo "Total JSON files: $TOTAL_COUNT"
