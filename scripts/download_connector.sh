#!/bin/bash
# Download the Solace Spark Connector JAR from Maven Central

VERSION="${1:-3.1.6}"
JAR_NAME="pubsubplus-connector-spark-${VERSION}.jar"
OUTPUT_DIR="${2:-./jars}"

mkdir -p "$OUTPUT_DIR"

URL="https://repo1.maven.org/maven2/com/solacecoe/connectors/pubsubplus-connector-spark/${VERSION}/${JAR_NAME}"

echo "Downloading Solace Spark Connector v${VERSION}..."
echo "URL: $URL"
echo "Output: ${OUTPUT_DIR}/${JAR_NAME}"

curl -L -o "${OUTPUT_DIR}/${JAR_NAME}" "$URL"

if [ $? -eq 0 ]; then
    echo "✓ Download complete: ${OUTPUT_DIR}/${JAR_NAME}"
    echo ""
    echo "To use with spark-submit:"
    echo "  spark-submit --jars ${OUTPUT_DIR}/${JAR_NAME} your_script.py"
    echo ""
    echo "To use with the consumer CLI:"
    echo "  python -m solace_consumer --connector-jar ${OUTPUT_DIR}/${JAR_NAME}"
else
    echo "✗ Download failed"
    exit 1
fi
