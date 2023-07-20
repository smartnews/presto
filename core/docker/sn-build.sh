# Smartnews has update code for some of the modules, so need to build
# and install them locally before we build the docker image.

JAVA_VER=$(java -version 2>&1 | sed -n ';s/.* version "\(.*\)\.\(.*\)\..*".*/\1\2/p;')
if [ "$JAVA_VER" -ge 170 ]; then
    echo "ok, java is 17 or newer"
else
    echo "java is too old, need java 17+"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "${SCRIPT_DIR}" || exit 2

# Build trino-smartnews-functions
mvn -f ../../plugin/trino-smartnews-functions/pom.xml clean install
# Build trino-kafka
mvn -f ../../plugin/trino-kafka/pom.xml clean install -DskipTests
# Build trino-iceberg
mvn -f ../../plugin/trino-iceberg/pom.xml clean install -DskipTests
# Build lib/trino-hive-formats
mvn -f ../../lib/trino-hive-formats/pom.xml clean install -DskipTests

# Build the trino-server and trino-cli modules
mvn -f ../trino-server/pom.xml clean install -DskipTests
mvn -f ../../client/trino-cli/pom.xml clean install -DskipTests

# Build the docker image
sh ./build.sh
