FULL_PATH_TO_SCRIPT="$(realpath "${BASH_SOURCE[0]}")"
SCRIPT_DIRECTORY="$(dirname "$FULL_PATH_TO_SCRIPT")"

if [ -z "$1" ]; then
  echo "Error: provide test case number as first argument"
  exit 1
fi

curl http://127.0.0.1:1234/test/run \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$(cat ${SCRIPT_DIRECTORY}/test-cases/${1}.json)"