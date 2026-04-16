#!/bin/bash
set -e

# Auto-detect JAVA_HOME for ARM or x86
if [ -d "/usr/lib/jvm/java-21-openjdk-arm64" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-arm64"
elif [ -d "/usr/lib/jvm/java-21-openjdk-amd64" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
else
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
fi

export PATH="${JAVA_HOME}/bin:${PATH}"

echo "=================================================="
echo "ETL Pipeline Starting"
echo "JAVA_HOME: ${JAVA_HOME}"
echo "Python: $(python3 --version)"
echo "Spark: $(python3 -c 'import pyspark; print(pyspark.__version__)')"
echo "=================================================="

# Run the ETL pipeline
exec python3 /app/src/main.py
