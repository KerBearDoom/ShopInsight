# =============================================================================
# ShopInsight 业务服务层（Spring Boot 后端 + Web 看板）
#
# 构建：docker compose build api
# 运行：docker compose up -d
#
# ⚠️ 构建前必须先有 jar：mvn clean package -DskipTests
#    （不做多阶段构建，原因见下）
# =============================================================================

# ── 基础镜像 ─────────────────────────────────────────────────────────────────
#
# 写成 ARG 是为了换基础镜像时只改一行，或构建时覆盖：
#     docker compose build --build-arg BASE_IMAGE=xxx api
#
# ★ 选镜像时踩过的坑，值得记下来：
#   第一次下载的文件叫
#       library-eclipse-temurin-17-jre-noble-linux-arm64-v8.image.tar
#   —— 文件名里明明写着 arm64，内容却是 amd64。
#   三处证据一致：index.json 的 architecture 字段、config blob 的 architecture 字段、
#   以及 Docker 运行时打印的「platform does not match host」警告。
#
#   判断一个镜像的架构，**不要看文件名**，看这两个之一：
#       docker image inspect <镜像> --format '{{.Architecture}} {{.Variant}}'
#       输出 `arm64 v8` 才是对的（amd64 的 variant 是空的）
#
#   另外：从 `arm64v8/` 这种**架构专用命名空间**下载，比从多架构 tag 里挑更不容易拿错。
#
# 当前这个已核实：architecture=arm64、variant=v8、Java 17.0.20。
# ubi9-minimal 是红帽的精简基础镜像，当运行时基础体积合适。
ARG BASE_IMAGE=arm64v8/eclipse-temurin:17-jre-ubi9-minimal
FROM ${BASE_IMAGE}

# ── 为什么不做多阶段构建 ─────────────────────────────────────────────────────
#
# 常见写法是「maven 镜像里编译 → JRE 镜像里运行」。这里不这么做，两个原因：
#   1. jar 已经构建好了，再引一个 maven 基础镜像纯属浪费体积
#   2. 这台机器的网络拉不动 Docker Hub，每多一个基础镜像就多一份拉不动的风险
#
# 代价：构建前必须先跑 mvn package。这一点写在 README 的启动步骤里。

WORKDIR /app

# .dockerignore 里已经把 Flutter 工程、.git、docs 等都排除了，
# 只放行 target/ 下这一个 jar
COPY target/shop-insight-0.0.1-SNAPSHOT.jar /app/app.jar

# ClickHouse 连接地址由环境变量注入。
#
# 默认值保持「本地开发」的写法（localhost），compose 里会覆盖成服务名 clickhouse。
# 这样同一个 jar 本地 java -jar 跑和容器跑都能用，不用改代码。
ENV CLICKHOUSE_URL="jdbc:clickhouse://localhost:8123/shop_insight"

EXPOSE 8080

# ★ 用 exec 形式（JSON 数组）而不是 shell 形式，这样 java 是容器的 PID 1，
#   能直接收到 docker stop 发的 SIGTERM 并优雅退出；
#   shell 形式会让 /bin/sh 成为 PID 1，java 收不到信号，只能等超时被 kill。
#
# -XX:MaxRAMPercentage=75 让 JVM 按容器内存限额的 75% 设堆上限。
# 不设的话 JVM 在容器里可能按宿主机内存估算，堆开太大被 OOM kill。
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
