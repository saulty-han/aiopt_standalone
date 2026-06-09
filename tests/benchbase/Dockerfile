# BenchBase MySQL Profile
#
# 构建: docker build -t benchbase-mysql tests/benchbase/
# 用法: docker run --rm --network host \
#          -v $(pwd)/tests/benchbase/tpch_config.xml:/benchbase/config/tpch_config.xml \
#          benchbase-mysql \
#          -b tpch -c config/tpch_config.xml --create=true --load=true --execute=false

FROM eclipse-temurin:23-jdk AS builder
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN git clone --depth 1 https://github.com/cmu-db/benchbase.git . \
    && ./mvnw clean package -P mysql -DskipTests -q \
    && tar xzf target/benchbase-mysql.tgz -C /opt

FROM eclipse-temurin:23-jre
WORKDIR /benchbase
COPY --from=builder /opt/benchbase-mysql/ .
ENTRYPOINT ["java", "-jar", "benchbase.jar"]
CMD ["--help"]
