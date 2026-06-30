FROM ghcr.io/actions/actions-runner:2.334.0

USER root

COPY --from=mcr.microsoft.com/dotnet/sdk:9.0 /usr/share/dotnet /usr/share/dotnet
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
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSfL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq \
    && chmod +x /usr/local/bin/yq

RUN python --version && python -c "import yaml; print('PyYAML installed successfully')"
RUN node --version && npm --version

# ✅ Устанавливаем pnpm глобально через npm (не через corepack)
RUN npm install -g pnpm@10.11.0

# ✅ Отключаем corepack полностью (чтобы не пытался обновлять pnpm)
ENV COREPACK_ENABLE=0
ENV COREPACK_ENABLE_AUTO_PIN=0

# ✅ Переключаемся на пользователя runner
USER runner

# ✅ Проверяем, что pnpm работает
RUN pnpm --version