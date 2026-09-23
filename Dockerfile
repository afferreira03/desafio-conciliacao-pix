# syntax=docker/dockerfile:1

# ---------- build: compila e gera o jar (testes rodam na esteira, não no build da imagem) ----------
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

# Dependências primeiro: só são baixadas de novo quando o pom muda.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package \
    && cp target/conciliacao-pix-*.jar application.jar

# Camadas do Spring Boot: dependências (mudam pouco) separadas do código da aplicação (muda sempre).
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

# ---------- runtime: só a JRE, usuário sem privilégios ----------
FROM eclipse-temurin:25-jre
WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --no-create-home app

COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./

USER app
EXPOSE 8081

# A JVM respeita o limite de memória do container; 75 % para heap, o resto para metaspace, threads e buffers.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["java", "-jar", "application.jar"]