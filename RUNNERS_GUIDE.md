# Docker Runner Scale Sets: развертывание и эксплуатация

Эта инструкция описывает текущую реализацию autoscaling self-hosted runners из этого репозитория. Она включает готовые примеры для `vneocheredi` и `migrant-exam-flow`, а также универсальную справочную часть для добавления новых scale set.

> [!IMPORTANT]
> Это самостоятельная Docker-реализация на базе `github.com/actions/scaleset`, а не стандартная Kubernetes-установка Actions Runner Controller (ARC). Требования GitHub к правам учетных данных применимы, но развертывание и жизненный цикл реализованы кодом этого проекта.

## 1. Что развертывает проект

На VM постоянно работает один небольшой controller-контейнер на каждый scale set. Controller:

1. подключается к GitHub и находит scale set по `--name` либо создает его при первом запуске;
2. слушает очередь заданий GitHub;
3. создает требуемое количество временных runner-контейнеров;
4. для каждого runner создает отдельные DinD-контейнер, Docker-сеть и workspace-volume;
5. удаляет всю временную пару после завершения job;
6. при своем следующем старте удаляет оставшиеся ресурсы этого scale set после аварии или перезагрузки VM.

Scale set в GitHub при остановке controller не удаляется. При следующем запуске controller повторно использует тот же scale set по сочетанию имени и runner group.

Упрощенная схема:

```text
GitHub Actions
      |
      | HTTPS: очередь заданий, JIT-конфигурация
      v
controller container
      |
      | /var/run/docker.sock
      v
Docker Engine основной VM
      |
      +-- <name>-runner-<id> ----+
      |                          | отдельная сеть <name>-net-<id>
      +-- <name>-dind-<id> ------+ DNS alias: docker
      |
      +-- <name>-workspace-<id>    общий workspace runner и DinD

Внутри runner:
docker CLI -> DOCKER_HOST=tcp://docker:2375 -> dockerd внутри своего DinD
```

## 2. Требования к VM

Рекомендуется Linux VM с:

- Docker Engine;
- Docker Compose v2, то есть командой `docker compose`;
- исходящим HTTPS-доступом к `github.com`, `api.github.com`, адресам GitHub Actions и используемому registry;
- доступом к `registry.ft-soft.ru` для controller-образа и runner-образов;
- достаточным запасом RAM и диска под параллельные job, DinD layers и build cache;
- `cron` для периодической очистки неиспользуемых образов и build cache.

Проверка:

```bash
docker version
docker compose version
docker info
command -v docker
systemctl is-active docker
```

Открывать входящий TCP-порт Docker daemon не требуется. Controller использует Unix socket VM, а порт `2375` DinD доступен только внутри приватной Docker-сети конкретной пары runner/DinD и не публикуется на VM.

## 3. Рекомендуемая структура на сервере

```text
/opt/scale-set-runners/
├── vneocheredi/
│   ├── docker-compose.yml
│   └── .env
├── maxexam/
│   ├── docker-compose.yml
│   └── .env
├── secrets/
│   └── vneocheredi/
│       └── key.pem
└── auto-scripts/
    └── auto-cleaner-cache.sh
```

Создание каталогов:

```bash
sudo mkdir -p \
  /opt/scale-set-runners/vneocheredi \
  /opt/scale-set-runners/maxexam \
  /opt/scale-set-runners/secrets/vneocheredi \
  /opt/scale-set-runners/auto-scripts \
  /opt/build-cache \
  /opt/build-cache-npm
```

Runner в текущем backend-образе работает от UID `1001`, поэтому каталоги build cache должны быть ему доступны:

```bash
sudo chown -R 1001:1001 /opt/build-cache /opt/build-cache-npm
sudo chmod 0755 /opt/build-cache /opt/build-cache-npm
```

## 4. Аутентификация GitHub

Поддерживаются два способа:

1. GitHub App — рекомендуемый вариант для repository- и organization-level runners.
2. Personal Access Token — запасной вариант или вариант для enterprise-level runners.

Если переданы полностью валидные параметры GitHub App, код выбирает GitHub App. Если параметры App неполные, но присутствует `--token`, используется PAT.

Официальная справка GitHub: [Authenticating ARC to the GitHub API](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/authenticate-to-the-api).

### 4.1. GitHub App

Создайте GitHub App, принадлежащий нужной организации, и назначьте права:

| Уровень регистрации | Права GitHub App |
|---|---|
| Репозиторий, `--url=https://github.com/ORG/REPO` | Repository permissions: `Administration: Read and write`, `Metadata: Read-only`; Organization permissions: `Self-hosted runners: Read and write` |
| Организация, `--url=https://github.com/ORG` | Repository permissions: `Metadata: Read-only`; Organization permissions: `Self-hosted runners: Read and write` |

Затем:

1. установите App в организацию или выбранный репозиторий;
2. запишите Client ID и Installation ID;
3. сгенерируйте private key `.pem`;
4. поместите ключ на VM, не добавляя его в Git;
5. ограничьте права файла.

```bash
sudo install -o root -g root -m 0600 key.pem \
  /opt/scale-set-runners/secrets/vneocheredi/key.pem
```

Пример `.env`:

```dotenv
GITHUB_APP_CLIENT_ID=Iv1.example
GITHUB_APP_INSTALLATION_ID=12345678
REGISTRY_USER=harbor_robot_or_user
REGISTRY_PASSWORD=replace_me
```

Не записывайте содержимое PEM прямо в Compose. Текущий безопасный вариант — read-only mount и флаг `--app-private-key-file`.

> [!NOTE]
> GitHub App нельзя использовать для регистрации runners на enterprise-level URL. Для enterprise-level runners GitHub требует PAT classic.

### 4.2. PAT classic

Минимальные scopes по официальной документации GitHub:

| Уровень регистрации | Scope PAT classic |
|---|---|
| Репозиторий | `repo` |
| Организация | `admin:org` |
| Enterprise | PAT classic с правами администратора соответствующего enterprise/runner scope |

Пример `.env`:

```dotenv
GITHUB_TOKEN=replace_with_pat
REGISTRY_USER=harbor_robot_or_user
REGISTRY_PASSWORD=replace_me
```

### 4.3. Fine-grained PAT

GitHub документирует следующие права:

| Уровень регистрации | Fine-grained permissions |
|---|---|
| Репозиторий | Repository permissions: `Administration: Read and write` |
| Организация | Repository permissions: `Administration: Read`; Organization permissions: `Self-hosted runners: Read and write` |

Для fine-grained PAT обязательно выберите правильного resource owner и разрешите доступ к нужным репозиториям.

### 4.4. Правила хранения секретов

- Никогда не записывайте PAT, Harbor password или PEM в Git.
- Файл `.env` должен иметь права `0600` и быть исключен через `.gitignore`.
- PEM монтируйте с `:ro`.
- При попадании токена в историю Git немедленно отзовите его и создайте новый; удаления строки из текущего YAML недостаточно.
- Предпочитайте отдельную GitHub App и отдельного Harbor robot account для каждого окружения.
- Регулярно ротируйте токены и private keys.

## 5. Развертывание текущих scale set

### 5.1. Авторизация в Harbor

Controller сам передает `REGISTRY_USER` и `REGISTRY_PASSWORD` при pull runner/DinD images через Docker API. Но Compose должен сначала получить сам приватный controller-образ, поэтому Docker Engine VM тоже должен быть авторизован в Harbor:

```bash
echo "$REGISTRY_PASSWORD" | docker login registry.ft-soft.ru \
  --username "$REGISTRY_USER" \
  --password-stdin
```

Проверка:

```bash
docker pull registry.ft-soft.ru/devops/scale_set_runners:latest
docker pull registry.ft-soft.ru/devops/runner_vneocheredi_backend:latest
```

### 5.2. Основной `vneocheredi` через GitHub App

Скопируйте `devopse_scale_Set_runners/docker-compose.yml` в `/opt/scale-set-runners/vneocheredi/docker-compose.yml`, создайте рядом `.env`, установите PEM по пути из раздела выше и запустите:

```bash
cd /opt/scale-set-runners/vneocheredi
docker compose --project-name vneocheredi-runners config
docker compose --project-name vneocheredi-runners pull
docker compose --project-name vneocheredi-runners up -d
docker compose --project-name vneocheredi-runners logs -f --tail=200
```

Текущий Compose регистрирует scale set на уровне организации `https://github.com/VneOcheredi` с именем `vneocheredi_runner` и пользовательской меткой `vneocheredi_runner_backend`.

### 5.3. Пример `migrant-exam-flow` через PAT

Файл `devopse_scale_Set_runners/docker-compose(2).yml` является вторым примером. На сервере лучше переименовать его в обычный `docker-compose.yml` и хранить в отдельном каталоге:

```bash
cp 'devopse_scale_Set_runners/docker-compose(2).yml' \
  /opt/scale-set-runners/maxexam/docker-compose.yml
cd /opt/scale-set-runners/maxexam
chmod 0600 .env
docker compose --project-name maxexam-runners config
docker compose --project-name maxexam-runners pull
docker compose --project-name maxexam-runners up -d
docker compose --project-name maxexam-runners logs -f --tail=200
```

Этот пример регистрирует repository-level scale set для `https://github.com/ft-soft/migrant-exam-flow` с именем `vneocheredi_runner_maxexam`.

Если оба файла запускаются прямо из одного каталога репозитория, всегда указывайте и файл, и уникальное имя Compose project:

```bash
docker compose \
  --file devopse_scale_Set_runners/docker-compose.yml \
  --project-name vneocheredi-runners \
  up -d

docker compose \
  --file 'devopse_scale_Set_runners/docker-compose(2).yml' \
  --project-name maxexam-runners \
  up -d
```

Уникальный `--project-name` предотвращает смешивание состояния двух Compose-проектов. Уникальные `container_name` и `--name` обязательны независимо от этого.

### 5.4. Универсальный шаблон Compose

```yaml
services:
  scale-set-controller:
    image: registry.ft-soft.ru/devops/scale_set_runners:latest
    container_name: my-project-scale-set-controller
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /opt/scale-set-runners/secrets/my-project/key.pem:/run/secrets/github-app-key.pem:ro
    environment:
      REGISTRY_USER: ${REGISTRY_USER}
      REGISTRY_PASSWORD: ${REGISTRY_PASSWORD}
    command: >
      --runner-image="registry.ft-soft.ru/devops/my-runner:latest"
      --url="https://github.com/ORG/REPO"
      --name="my_project_runner"
      --labels="my_project_runner"
      --min-runners=1
      --max-runners=4
      --log-level=info
      --app-client-id=${GITHUB_APP_CLIENT_ID}
      --app-installation-id=${GITHUB_APP_INSTALLATION_ID}
      --app-private-key-file=/run/secrets/github-app-key.pem
```

## 6. Все флаги controller

Источник истины для флагов — `scale_set_runners_image/main.go` и `scale_set_runners_image/config.go`.

| Флаг | Обязательный | По умолчанию | Назначение и тонкости |
|---|---:|---|---|
| `--url` | Да | — | Полный URL репозитория, организации или enterprise, где регистрируется scale set. Примеры: `https://github.com/ORG/REPO`, `https://github.com/ORG`. |
| `--name` | Да | — | Постоянное имя scale set. Используется также как префикс Docker-ресурсов. Должно быть уникальным внутри runner group. Не меняйте без необходимости. |
| `--labels` | Нет | значение `--name` | Метка для выбора runner workflow. Cobra принимает список через запятую или повторение флага, но для совместимости runner scale sets рекомендуется одна метка. Пустая метка запрещена. |
| `--min-runners` | Нет | `0` | Минимальное число постоянно подготовленных ephemeral runners. Увеличивает скорость старта job, но расходует RAM. |
| `--max-runners` | Нет | `10` | Максимальное число одновременно созданных runners этим controller. Не может быть меньше `--min-runners`. |
| `--runner-group` | Нет | `default` | Имя существующей runner group. Для `default` код использует ID `1`; для остальных имен запрашивает group через GitHub API. |
| `--app-client-id` | Условно | — | Client ID GitHub App. Используется только вместе с installation ID и private key. |
| `--app-installation-id` | Условно | `0` | Числовой Installation ID установленной GitHub App. |
| `--app-private-key` | Условно | — | PEM целиком в аргументе. Нежелателен: секрет будет заметнее в конфигурации и списке процессов. Предпочитайте файл. |
| `--app-private-key-file` | Условно | — | Путь к PEM внутри controller-контейнера. При validation файл читается, пробелы по краям удаляются. |
| `--token` | Условно | — | PAT вместо GitHub App. Не передавайте литералом в YAML; используйте `${GITHUB_TOKEN}`. |
| `--runner-image` | Нет | `ghcr.io/actions/actions-runner:latest` | Образ временного runner. Для этого проекта обычно используется кастомный backend-образ из Harbor. |
| `--dind-image` | Нет | `docker:dind` | Образ отдельного Docker daemon для каждой runner-пары. Можно указать закрепленный тег или приватный mirror. |
| `--job-start-timeout` | Нет | `5m` | Максимальное ожидание старта назначенной GitHub job. После таймаута controller диагностирует сеть и контейнеры, затем безопасно выводит из регистрации один старейший idle runner и создает замену. Busy runners не затрагиваются; без ожидающих job таймер отключен. |
| `--log-level` | Нет | `info` | `debug`, `info`, `warn`, `error`. Неизвестное значение фактически превращается в `info`. |
| `--log-format` | Нет | `text` | `text` или `json`. Любое другое значение полностью отключает вывод логов. |

Должен быть передан полный набор GitHub App либо PAT. Если валиден полный набор App, он имеет приоритет над PAT.

Переменные окружения registry не являются CLI-флагами:

| Переменная | Назначение |
|---|---|
| `REGISTRY_USER` | Пользователь или robot account для pull `--runner-image` и `--dind-image`. |
| `REGISTRY_PASSWORD` | Пароль или token registry. |

Registry auth добавляется только когда переданы обе переменные и ссылка на образ содержит явный registry host, например `registry.ft-soft.ru/...`. Для `docker:dind` без явного host credentials не добавляются.

## 7. Использование runner в workflow

Обычно job должен ссылаться на имя scale set:

```yaml
jobs:
  build:
    runs-on: vneocheredi_runner
    steps:
      - uses: actions/checkout@v4
      - run: docker version
      - run: docker build -t local-build .
```

Если ваша текущая маршрутизация в GitHub настроена на пользовательскую метку, используйте значение из `--labels`. Не смешивайте метки разных scale set и проверьте фактическое имя/label в настройках GitHub Actions runners.

## 8. Внутренняя Docker- и сетевая система

### 8.1. Внешний Docker daemon VM

Controller получает bind mount:

```yaml
- /var/run/docker.sock:/var/run/docker.sock
```

Через этот socket он создает и удаляет внешние runner/DinD-контейнеры, сети и volumes. Это не Docker socket, который получает workflow.

> [!WARNING]
> Доступ к `/var/run/docker.sock` практически равен root-доступу к VM. Запускайте только доверенный controller-образ, ограничивайте доступ к серверу и registry и не передавайте этот socket непосредственно job-контейнеру.

### 8.2. Одна изолированная пара на каждый runner

Для каждого runner controller параллельно создает:

- сеть `<scale-set>-net-<8-symbol-id>`;
- volume `<scale-set>-workspace-<8-symbol-id>`;
- privileged DinD `<scale-set>-dind-<8-symbol-id>`;
- runner `<scale-set>-runner-<8-symbol-id>`.

Имя scale set нормализуется: неподдерживаемые символы заменяются `-`, пустой результат становится `scale-set`, а префикс ограничивается 48 символами.

На все управляемые ресурсы ставятся labels:

```text
ft-soft.runner-scale-set.managed=true
ft-soft.runner-scale-set.name=<полное имя scale set>
ft-soft.runner-scale-set.resource=runner|dind|network|workspace
```

Они позволяют отличать ресурсы разных scale set и очищать только принадлежащие текущему controller.

### 8.3. Почему адрес называется `docker`

DinD получает в своей приватной сети aliases:

```text
docker
<scale-set>-dind-<id>
```

Runner получает:

```text
DOCKER_HOST=tcp://docker:2375
```

Здесь `docker` — не имя VM и не глобальный сервер. Это DNS alias DinD-контейнера только внутри одной приватной сети. В другой runner-сети может существовать такой же alias `docker`, но Docker DNS разрешит его в другой DinD-контейнер. Поэтому параллельные runners и разные scale set не конфликтуют.

TLS внутри пары отключен через `DOCKER_TLS_CERTDIR=`. Это приемлемо только потому, что `2375` не публикуется наружу и сеть создается отдельно для одной пары.

### 8.4. Где создаются контейнеры workflow

Есть два уровня Docker:

| Уровень | Что создает | Что видно через `docker ps` |
|---|---|---|
| Docker Engine VM | controller | controller, внешние runner и DinD |
| dockerd внутри DinD | Docker CLI workflow | build-контейнеры, service containers и layers конкретной job |

Команда `docker ps` на VM не показывает внутренние контейнеры DinD. Для их просмотра:

```bash
docker exec <dind-container-name> docker ps -a
```

### 8.5. Общий workspace

Один named volume монтируется и в runner, и в DinD по пути `/home/runner/_work`. Поэтому Docker container actions и job-контейнеры видят checkout и рабочие файлы runner.

Дополнительно только runner получает host bind mounts:

```text
/opt/build-cache     -> /opt/build-cache
/opt/build-cache-npm -> /opt/build-cache-npm
```

Эти два cache-каталога переживают завершение ephemeral runner и используются несколькими job. Не храните там секреты.

### 8.6. Почему `actions/checkout` необходимо выполнять в каждой job

Нельзя выполнить `actions/checkout` один раз в одной job и затем обращаться к скачанному репозиторию из других job.

Каждая job запускается на отдельном ephemeral runner. Для нее controller создает собственные:

- runner-контейнер;
- DinD-контейнер;
- Docker-сеть;
- workspace-volume.

Поэтому рабочая директория одной job недоступна другим job. Если первая job выполнила `actions/checkout`, репозиторий будет скачан только в ее workspace. Параллельно запущенные или последующие job получат другие runners и пустые workspace, в которых файлов репозитория нет.

После завершения job ее runner и workspace-volume удаляются. Поэтому сохранить checkout и автоматически передать его следующей job также нельзя, даже если между job настроена зависимость через `needs`.

Например, следующая схема не работает:

```yaml
jobs:
  checkout:
    runs-on: vneocheredi_runner
    steps:
      - uses: actions/checkout@v4

  build:
    needs: checkout
    runs-on: vneocheredi_runner
    steps:
      - run: docker build .
```

Job `build` запускается на новом runner. В ее workspace отсутствуют файлы, скачанные job `checkout`, поэтому команды могут завершаться ошибками:

```text
Dockerfile not found
package.json not found
project file does not exist
no such file or directory
```

Правильный вариант — выполнять `actions/checkout` в каждой job, которой нужны исходники:

```yaml
jobs:
  build:
    runs-on: vneocheredi_runner
    steps:
      - uses: actions/checkout@v4
      - run: docker build .

  test:
    runs-on: vneocheredi_runner
    steps:
      - uses: actions/checkout@v4
      - run: ./run-tests.sh
```

Один `actions/checkout` можно использовать для нескольких последовательных steps только внутри одной job, поскольку все steps этой job выполняются на одном runner и используют один workspace.

Если между job нужно передать не весь Git-репозиторий, а результат сборки, используйте `actions/upload-artifact` и `actions/download-artifact`. Cache предназначен для зависимостей и ускорения сборки, но не должен использоваться как общая рабочая копия репозитория.

### 8.7. Безопасность DinD

DinD запускается с `Privileged: true`, иначе вложенный Docker daemon обычно не сможет работать. Изоляция защищает основной Docker daemon от прямого доступа workflow, но privileged DinD все равно увеличивает риск. Не запускайте недоверенные pull request jobs из forks на этих runners без отдельной оценки угроз и ограничений GitHub environments/approvals.

## 9. Масштабирование и жизненный цикл

Controller вычисляет целевое число runners так:

```text
target = min(max-runners, min-runners + requested-by-GitHub)
```

Пример для `--min-runners=1 --max-runners=4`:

| Запрос GitHub | Целевое число runners |
|---:|---:|
| `0` | `1` |
| `1` | `2` |
| `3` | `4` |
| `10` | `4` |

Недостающие runners запускаются параллельно. Для каждого запуска:

1. создаются сеть и workspace-volume;
2. запускается DinD и ожидается его healthcheck, максимум 45 секунд;
3. GitHub выдает одноразовую JIT-конфигурацию;
4. запускается runner;
5. controller ждет healthcheck или строку `Listening for Jobs`, максимум 90 секунд;
6. после события `JobCompleted` runner, DinD, volume и сеть принудительно удаляются.

Runner является ephemeral. Нельзя рассчитывать на сохранение файлов внутри его container filesystem между job. Сохраняйте результаты в artifacts, внешнем хранилище или специально предназначенном cache.

### Очистка после сбоя или перезапуска VM

При старте controller до запуска listener выполняется `cleanupStaleResources`. Он ищет контейнеры, volumes и networks текущего scale set по labels и совместимому префиксу имени. Ресурсы других scale set не затрагиваются.

После очистки controller повторно использует существующий GitHub scale set по `--name`; удалять и создавать scale set вручную при каждом рестарте не нужно.

Встроенный volume janitor каждые 3 минуты проверяет dangling volumes, похожие на workspace. Он не трогает отслеживаемые активные volumes и обычно удаляет кандидата только старше 10 минут.

## 10. Управление и обновление

В командах ниже выполняйте операции из каталога нужного Compose-проекта либо передавайте `--file` и `--project-name` явно.

Состояние и логи:

```bash
docker compose ps
docker compose logs --tail=200
docker compose logs -f
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
docker network ls
docker volume ls
```

Безопасная остановка с временем на обработку `SIGTERM` и очистку runner-пар:

```bash
docker compose down --timeout 60
```

Controller обрабатывает `SIGINT` и `SIGTERM`. Не используйте `docker kill -s KILL` для штатной остановки: при `SIGKILL` cleanup не выполняется, а остатки будут удалены только при следующем старте controller.

Запуск после остановки:

```bash
docker compose up -d
docker compose logs -f --tail=200
```

Обновление controller-образа:

```bash
docker compose pull
docker compose down --timeout 60
docker compose up -d
docker compose logs -f --tail=200
```

После reboot благодаря `restart: unless-stopped` controller стартует автоматически, очищает старые пары и подключается к постоянному scale set. Если Compose service был ранее явно остановлен командой `docker compose stop`, политика `unless-stopped` может сохранить остановленное состояние; проверьте `docker ps -a` и выполните `docker compose up -d`.

## 11. Периодическая очистка через cron

### 11.1. Установка скрипта

```bash
sudo install -o root -g root -m 0755 \
  devopse_scale_Set_runners/auto-cleaner-cache.sh \
  /opt/scale-set-runners/auto-scripts/auto-cleaner-cache.sh
```

Проверьте фактический путь Docker:

```bash
command -v docker
```

На большинстве серверов это `/usr/bin/docker`. У cron сокращенный `PATH`, а текущий скрипт вызывает просто `docker`, поэтому задайте `PATH` в crontab.

Откройте root crontab, поскольку очистка Docker и запись в `/var/log` обычно требуют root:

```bash
sudo crontab -e
```

Рекомендуемый блок:

```cron
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

0 4 * * * /opt/scale-set-runners/auto-scripts/auto-cleaner-cache.sh
0 14 * * * /usr/bin/docker image prune --force >> /var/log/docker-prune.log 2>&1 && /usr/bin/docker buildx prune --force >> /var/log/docker-prune.log 2>&1
```

Расписание:

- `0 4 * * *` — ежедневно в 04:00 запускается `auto-cleaner-cache.sh`;
- `0 14 * * *` — ежедневно в 14:00 безусловно удаляются dangling images и неиспользуемый buildx cache;
- время берется из timezone VM, проверьте его командой `timedatectl`.

Текущий скрипт в 04:00:

1. вычисляет процент свободной RAM;
2. при значении ниже `20%` выполняет `docker image prune --all --force`;
3. затем выполняет `docker buildx prune --all --force`;
4. пишет результат в `/var/log/memory-clean.log`.

> [!IMPORTANT]
> Скрипт принимает решение по свободной оперативной памяти, однако `image prune` и `buildx prune` освобождают прежде всего дисковое пространство, а не RAM. Если цель — защита диска от заполнения, условие скрипта следует в будущем перевести на проверку `df` или `docker system df`.

Разница команд:

| Команда | Что удаляет |
|---|---|
| `docker image prune --force` | Только dangling images, не привязанные к тегам и контейнерам. |
| `docker image prune --all --force` | Все неиспользуемые контейнерами images, включая тегированные. При следующем старте они будут скачаны заново. |
| `docker buildx prune --force` | Неиспользуемый build cache выбранного builder. |
| `docker buildx prune --all --force` | Более агрессивно удаляет весь неиспользуемый build cache. |

Эта очистка относится к Docker Engine VM. Она не очищает images и layers внутри уже работающих DinD-контейнеров. DinD целиком удаляется после завершения соответствующей job.

Не добавляйте `docker volume prune` или `docker system prune --volumes` без отдельного анализа: на VM могут находиться volumes других проектов. Workspace-volumes данного проекта controller очищает самостоятельно.

Проверка cron:

```bash
sudo /opt/scale-set-runners/auto-scripts/auto-cleaner-cache.sh
sudo tail -n 100 /var/log/memory-clean.log
sudo tail -n 100 /var/log/docker-prune.log
sudo systemctl status cron || sudo systemctl status crond
sudo crontab -l
```

## 12. Сборка и публикация controller-образа

PowerShell-скрипт в корне собирает `scale_set_runners_image/Dockerfile` и публикует образ в `registry.ft-soft.ru/devops/scale_set_runners`.

На Windows/PowerShell:

```powershell
docker login registry.ft-soft.ru
.\build-scale-set-runners.ps1 -Tag latest
```

С версионным тегом:

```powershell
.\build-scale-set-runners.ps1 -Tag dind-par-v2
```

Скрипт принимает только допустимый Docker tag, выполняет `docker build`, затем `docker push`. Проверка публикации:

```powershell
docker buildx imagetools inspect registry.ft-soft.ru/devops/scale_set_runners:latest
```

Для production лучше публиковать неизменяемый версионный tag и указывать его в Compose вместо плавающего `latest`.

## 13. Диагностика

### Controller не запускается

```bash
docker compose config
docker compose ps -a
docker compose logs --tail=300
docker inspect <controller-name> --format '{{json .State}}'
```

Проверьте:

- существует ли `.env` и подставились ли переменные;
- читается ли PEM внутри container mount;
- установлен ли App в правильную организацию/репозиторий;
- совпадает ли `--url` с областью выданных прав;
- доступен ли `/var/run/docker.sock`;
- авторизован ли Docker VM в Harbor.

Не публикуйте вывод `docker compose config` в тикеты без проверки: после интерполяции он может содержать секреты.

### Ошибка pull из Harbor

```bash
docker login registry.ft-soft.ru
docker pull registry.ft-soft.ru/devops/scale_set_runners:latest
docker pull registry.ft-soft.ru/devops/runner_vneocheredi_backend:latest
```

Убедитесь, что `REGISTRY_USER` и `REGISTRY_PASSWORD` переданы в `environment` controller. Login Docker VM нужен для controller image, а эти переменные нужны controller для pull runner image через Docker API.

### Job остается в очереди

Проверьте:

1. имя scale set или label в `runs-on`;
2. доступ репозитория к runner group;
3. `--min-runners`/`--max-runners`;
4. логи listener и scaler;
5. лимиты CPU, RAM, disk и Docker address pools на VM.

```bash
docker compose logs --tail=500 | grep -E \
  'Created runner scale set|Reusing existing runner scale set|Scaling up|assigned-job|GitHub DNS|GitHub HTTPS|network probe|recycl|failed|error'
```

При положительном сигнале GitHub controller пишет `Received assigned-job signal`. Если job не стартовала за `--job-start-timeout` (по умолчанию пять минут), появится `Assigned job did not start before watchdog timeout`. Перед безопасной заменой одного idle runner в лог попадут:

- состояние и health runner/DinD-контейнеров;
- последние 200 строк их логов;
- результаты DNS и HTTPS-проверок `github.com` и `api.github.com` из controller;
- результат аналогичной проверки из runner-контейнера;
- имя выведенного из регистрации и нового runner.

Перед удалением Docker-контейнера controller сначала удаляет регистрацию выбранного idle runner в GitHub, затем оставляет короткое окно для гонки с `JobStarted` и посылает только корректный `SIGTERM`. До `SIGKILL` безопасный режим не эскалирует. Контейнер очищается только после подтвержденного выхода. Если runner успел получить `JobStarted` во время окна гонки, он переводится в `busy` и продолжает job. Если уже выведенный из регистрации контейнер не завершился за две минуты, controller может запустить replacement и продолжит наблюдать за старым контейнером; число зарегистрированных в GitHub runners при этом остается в пределах `--max-runners`.

После одной замены повторный пятиминутный отсчет не запускается по старому событию автоматически. Для следующей попытки listener должен снова подтвердить положительным сигналом, что job всё ещё ожидает runner. Поэтому одиночное устаревшее событие не создает бесконечный цикл рестартов.

Пустой ответ long-poll от GitHub означает только отсутствие новой информации и вообще не передается watchdog как `count=0`. Таймер изменяется лишь по начальной статистике сессии или реальному сообщению GitHub. Поэтому пустой poll не сбрасывает уже запущенный отсчет, а настоящее значение `TotalAssignedJobs=0` корректно его отключает.

`GitHub DNS probe failed` указывает на DNS/сетевую проблему controller/VM. `GitHub HTTPS probe failed` при успешном DNS обычно означает сбой маршрута, proxy, TLS или firewall. Если проверки controller успешны, а `In-runner GitHub network probe completed` имеет ненулевой `exitCode`, проблема локализована в Docker-сети runner. Обычный `ping` не используется: ICMP может блокироваться независимо от HTTPS.

### DinD не становится healthy

```bash
docker ps -a --filter 'name=-dind-'
docker logs <dind-container-name>
docker inspect <dind-container-name> --format '{{json .State.Health}}'
```

Проверьте наличие `privileged`, свободный диск, возможность скачать `--dind-image` и отсутствие системных ограничений nested containers.

### Остались старые ресурсы

Сначала перезапустите controller: startup cleanup должен удалить ресурсы именно его scale set.

```bash
docker compose down --timeout 60
docker compose up -d
docker compose logs --tail=300
```

Просмотр по labels:

```bash
docker ps -a --filter label=ft-soft.runner-scale-set.managed=true
docker network ls --filter label=ft-soft.runner-scale-set.managed=true
docker volume ls --filter label=ft-soft.runner-scale-set.managed=true
```

Не удаляйте все Docker-ресурсы VM общей командой, если на ней работают другие проекты.

## 14. Контрольный чек-лист нового scale set

- [ ] Созданы отдельные `--name`, `container_name` и Compose `--project-name`.
- [ ] `--url` указывает на правильный repository/organization/enterprise scope.
- [ ] GitHub App или PAT имеет минимально необходимые права.
- [ ] App установлена в нужную организацию/репозиторий.
- [ ] PEM и `.env` не находятся в Git и имеют ограниченные права.
- [ ] Docker VM авторизован в приватном registry.
- [ ] Controller получает `REGISTRY_USER` и `REGISTRY_PASSWORD`.
- [ ] Каталоги `/opt/build-cache*` существуют и доступны UID `1001`.
- [ ] `--min-runners` не больше `--max-runners`.
- [ ] Runner image содержит `/home/runner/run.sh` и требуемые инструменты job.
- [ ] `runs-on` соответствует имени/метке scale set.
- [ ] `docker compose config`, `pull`, `up -d` и логи проверены.
- [ ] Выполнена тестовая workflow с `docker version` и простой сборкой.
- [ ] Настроены cron, log rotation и контроль свободного места.
- [ ] Проверена штатная остановка через `docker compose down --timeout 60`.

## 15. Файлы проекта

| Файл | Ответственность |
|---|---|
| `scale_set_runners_image/main.go` | CLI-флаги, GitHub scale set, listener, сигналы остановки, подключение Docker client. |
| `scale_set_runners_image/config.go` | Значения по умолчанию, validation, выбор GitHub App/PAT, logger и registry auth. |
| `scale_set_runners_image/scaler.go` | Масштабирование, runner/DinD, сети, volumes, healthchecks и cleanup. |
| `scale_set_runners_image/Dockerfile` | Сборка controller binary и controller image. |
| `runners_build_image/build_backend_runner_image/Dockerfile` | Кастомный backend runner image и установленные инструменты. |
| `devopse_scale_Set_runners/docker-compose.yml` | Текущий пример organization-level scale set через GitHub App. |
| `devopse_scale_Set_runners/docker-compose(2).yml` | Дополнительный repository-level пример через PAT. |
| `devopse_scale_Set_runners/auto-cleaner-cache.sh` | Условная очистка host images/build cache. |
| `build-scale-set-runners.ps1` | Сборка и публикация controller image в Harbor. |

## Официальные ссылки

- [Runner scale sets](https://docs.github.com/en/actions/concepts/runners/runner-scale-sets)
- [Authenticating ARC to the GitHub API](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/authenticate-to-the-api)
- [Self-hosted runners REST API](https://docs.github.com/en/rest/actions/self-hosted-runners)
- [Docker image prune](https://docs.docker.com/reference/cli/docker/image/prune/)
- [Docker buildx prune](https://docs.docker.com/reference/cli/docker/buildx/prune/)
