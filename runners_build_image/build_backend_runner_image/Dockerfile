FROM ghcr.io/actions/actions-runner:latest

USER root

COPY --from=udv-runners-nexus-proxy.ft-soft.ru/microsoft_cache/dotnet/sdk:9.0 /usr/share/dotnet /usr/share/dotnet
ENV PATH="/usr/share/dotnet:${PATH}"

RUN apt-get update \
    && apt-get install -y \
        docker.io \
        python3 \
        python3-pip \
        python3-venv \
        python3-yaml \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    # Node.js 20 LTS через NodeSource
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/* \
    # pnpm через corepack (встроен в Node.js 16.9+)
    && corepack enable \
    && corepack prepare pnpm@latest --activate

RUN curl -sSfL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq \
    && chmod +x /usr/local/bin/yq
# Проверка
RUN python --version && python -c "import yaml; print('PyYAML installed successfully')"
RUN node --version && npm --version && pnpm --version

USER runner