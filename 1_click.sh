SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pushd "${SCRIPT_DIR}" 1> /dev/null

# Check if script has execute permission and add it if necessary
if [ ! -x "$0" ]; then
  echo "Adding execute permission to 1_click.sh..."
  chmod +x "$0"
fi

echo -n "Please wait..."
git submodule update --init --recursive &> /dev/null || true
echo -e -n "\r                \r"

chmod +x src/utils/interactive/interactive.sh
# Run interactive.sh with necessary arguments
src/utils/interactive/interactive.sh "${@}"
# src/utils/interactive/interactive.sh "${1}" "${2}"
popd 1> /dev/null