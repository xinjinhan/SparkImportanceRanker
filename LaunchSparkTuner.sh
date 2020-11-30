#!/bin/bash

MACHINE_TYPE="$(uname -m)"
MACHINE_IS_X86='no'
if [ "${MACHINE_TYPE}" = 'amd64' -o "${MACHINE_TYPE}" = 'x86_64' -o "${MACHINE_TYPE}" = 's390x' ]; then
	MACHINE_IS_X86='yes'
fi

MACHINE_IS_ARM='no'
if [ "${MACHINE_TYPE}" = 'arm' -o "${MACHINE_TYPE}" = 'armv7l' -o "${MACHINE_TYPE}" = 'armv8l'  -o "${MACHINE_TYPE}" = 'aarch' -o "${MACHINE_TYPE}" = 'aarch64' ]; then
	MACHINE_IS_ARM='yes'
fi

echo $MACHINE_TYPE

if [ "$MACHINE_IS_X86" = 'yes' ]; then
  $(pwd)/PyEnv/X86/bin/python3 $(pwd)/EntrySparkTuner.py
fi

if [ "$MACHINE_IS_ARM" = 'yes' ]; then
  $(pwd)/PyEnv/ARM/bin/python3 $(pwd)/EntrySparkTuner.py
fi