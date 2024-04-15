rm -rf build
gradle build

export MINIO_PASSWORD="RootRoot"
export MINIO_USER="root"
export MINIO_URI="http://62.169.24.195:9000"
export NEO4J_DB="neo4j"
export NEO4J_URI="neo4j://62.169.24.194"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="Pjws94319431"

java -jar build/libs/server-0.0.1-SNAPSHOT.jar
