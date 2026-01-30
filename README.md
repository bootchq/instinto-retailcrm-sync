# RetailCRM → Google Sheets: выгрузка чатов (WhatsApp + Instagram) и анализ менеджеров

> Связано: [ТЗ](ТЗ_выгрузка_чатов_RetailCRM.md) | [Архитектура](Архитектура.md) | [Бэклог](Бэклог.md) | 

---


## Что делает

- Выгружает чаты/сообщения из RetailCRM за период (например, 2–3 месяца).
- Пишет в Google Sheets:
  - `chats_raw` — 1 строка = 1 чат (метаданные)
  - `messages_raw` — 1 строка = 1 сообщение
  - `manager_summary` — сводка по менеджерам
  - `channel_summary` — сводка по каналам
  - `chat_advice` — советы по каждому чату (rule-based)

## Важно про API RetailCRM

Раздел **«Чаты»** в RetailCRM может быть реализован через разные модули/интеграции. Поэтому в проекте есть файл `retailcrm_chats/retailcrm_endpoints.py` — там **1 место**, где при необходимости корректируются пути эндпоинтов.

Если после запуска вы увидите ошибку 404/403 на методах чатов — пришлите мне 1 пример ответа/ошибки, и я подстрою адаптер под ваш аккаунт.

## Подготовка

### 1) RetailCRM

Нужно:
- `RETAILCRM_URL` — например `https://yourdomain.retailcrm.ru`
- `RETAILCRM_API_KEY` — ключ доступа к API

### 2) Google Sheets

Способ №1 (рекомендую): **Service Account**

- Создайте service account в Google Cloud (Sheets API).
- Скачайте JSON ключ (например `service_account.json`).
- Создайте Google-таблицу и **поделитесь** ею на email service account (в JSON есть поле `client_email`).

Нужно:
- `GOOGLE_SHEETS_ID` — id таблицы (из URL)
- `GOOGLE_SERVICE_ACCOUNT_JSON` — путь к JSON файлу

## Самый простой способ настроить запуск (файл `env`)

В этой папке лежит `env.example`. Скопируйте его в `env` и заполните значения — скрипты сами подхватят переменные.

## Установка

```bash
cd retailcrm_chats
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск (разовая выгрузка)

```bash
# если файл env называется иначе:
# export RETAILCRM_ENV_FILE="/abs/path/to/env"
python export_to_sheets.py
```

## Быстрая проверка API (рекомендую перед выгрузкой)

```bash
# если файл env называется иначе:
# export RETAILCRM_ENV_FILE="/abs/path/to/env"
python probe_retailcrm_api.py
```

## Если “Чаты” не доступны через /api/v5/* (как у вас)

Это нормально: в некоторых аккаунтах RetailCRM “Чаты” доступны только через web-endpoint'ы интерфейса.

Тогда делаем так:

1) Открой страницу чатов: [`https://instinto.retailcrm.ru/chats`](https://instinto.retailcrm.ru/chats)
2) DevTools → Network → Fetch/XHR
3) Кликни на любой чат (и/или обнови страницу)
4) Найди запрос, который возвращает JSON со списком чатов или сообщений
5) Right click → Copy → **Copy as cURL (bash)**
6) Вставь это в файл `web_curl.txt` в папке проекта (не присылай содержимое в чат — там cookies)

Проверка:

```bash
python probe_web_chats.py
```

После того как `probe_web_chats.py` выдаёт ответ 200, можно выгружать **список чатов** в Google Sheets командой:

```bash
python export_to_sheets.py
```

Примечание: в MVP выгрузка сообщений берёт последние сообщения (`lastMessage`/`lastNotSystemMessage`). Для полной истории сообщений добавим второй `Copy as cURL` с запросом списка сообщений внутри чата.

### Полная история сообщений (через HAR)

У вас уже работает HAR-парсер:

1) DevTools → Network → открой чат → пролистай ленту вверх  
2) Export HAR (with content) → положи файл в `Cursor/INSTINTO/Сырье/*.har`
3) Запусти:

```bash
python har_find_messages_batch.py
python probe_web_messages.py
python export_to_sheets.py
```

`export_to_sheets.py` автоматически использует `web_messages_curl.txt` (если он есть) и будет тянуть сообщения через operationName `messages`.

## Автопоиск запроса сообщений (без ручного “угадывания batch”)

Если сложно найти правильный `batch` вручную — сделай так:

1) DevTools → Network → Fetch/XHR → включи Preserve log  
2) Открой чат и **пролистай переписку вверх**, чтобы подгрузилась история  
3) В Network: **Export HAR (with content)** / “Save all as HAR with content”  
4) Сохрани файл в папку проекта как `chats.har`:
   - `.../retailcrm_chats/chats.har`
5) Запусти:

```bash
python har_find_messages_batch.py
python probe_web_messages.py
```

Скрипт сам создаст `web_messages_curl.txt` из HAR.

## Аудит таблицы (проверка “соответствует ли ожиданиям”)

Чтобы я мог проверить структуру/количество строк/вкладки, запусти:

```bash
python sheet_audit.py
```

Скрипт создаст файл `sheet_audit_report.json` (без утечки текстов/телефонов). Пришли мне его содержимое или просто скажи “файл появился”, и я сам прочитаю его из vault и дам вердикт.

## Отчёт по менеджерам (в Google Sheets)

Скрипт читает `manager_summary` / `channel_summary` и создаёт вкладку `manager_report` с ранжированием и “фокусом обучения”.

```bash
python manager_report.py
```

## Поведенческий анализ (то, что менеджеры ДЕЛАЮТ в переписке) + “через неделю сравнить”

Скрипт `behavior_digest.py` читает `messages_raw` и считает поведенческие метрики по менеджерам, а также кладёт примеры диалогов (короткие редактированные фрагменты).

Он создаст вкладки:
- `behavior_snapshot_managers` — текущие метрики
- `weekly_behavior_delta_managers` — дельта vs прошлый недельный снепшот
- `weekly_examples` — 3–5 примеров на менеджера (no_reply / slow_reply / no_next_step / good)

```bash
python behavior_digest.py
```

## Запуск в фоне + еженедельный запуск (понедельник)

### Важно про закрытую крышку

Если Mac уходит в sleep при закрытии крышки — любые процессы **ставятся на паузу**. Тут 3 варианта:
- держать Mac бодрствующим на зарядке (или clamshell mode с внешним монитором),
- запускать задачу, когда Mac точно включён,
- вынести прогон на сервер (самый надёжный вариант).

### Разовый запуск “в фоне” (если Mac не спит)

```bash
cd "/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats"
source .venv/bin/activate
nohup /usr/bin/caffeinate -dimsu -t 21600 bash -lc "python export_to_sheets.py && python weekly_digest.py && python sheet_audit.py" > run_bg.log 2>&1 &
```

### Еженедельно через launchd (каждый понедельник 08:10)

1) Дай права на запуск скрипта:

```bash
chmod +x "/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats/run_weekly.sh"
```

2) Установи launchd job:

```bash
mkdir -p ~/Library/LaunchAgents
cp "/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats/launchd/ru.instinto.retailcrm.weekly.plist" ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/ru.instinto.retailcrm.weekly.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/ru.instinto.retailcrm.weekly.plist
```

3) Логи выполнения:
- `weekly_run.out.log`
- `weekly_run.err.log`

### Что будет “сводкой в понедельник”

После прогона в Google Sheets появятся вкладки:
- `weekly_digest_managers` — изменения по менеджерам vs прошлый недельный снепшот
- `weekly_digest_channels` — изменения по каналам

## Одна команда (всё сразу)

Если не хочешь помнить порядок команд:

```bash
python run_all.py
```

## Авто-отчёт в Telegram (чтобы ничего не запускать руками)

1) Создай бота: `@BotFather` → `/newbot` → получишь `TELEGRAM_BOT_TOKEN`.
2) Узнай `TELEGRAM_CHAT_ID`:
   - Напиши боту в личку “test”
   - Запусти: `python telegram_get_chat_id.py` (выведет chat.id)
   - (или в браузере: `https://api.telegram.org/botTOKEN/getUpdates` — ВАЖНО: без `<` и `>`)
3) В `env` включи:
   - `TELEGRAM_ENABLED="1"`
   - `TELEGRAM_BOT_TOKEN="8324229923:AAFDMCvLoWDagiqV2vRdh7cfx2lyw_PmThQ"`
   - `TELEGRAM_CHAT_ID="..."`

После этого еженедельный `launchd`-прогон сам отправит сводку (скрипт `send_weekly_telegram.py`).

## Что считается “советом”

Сейчас советы rule-based (без ИИ), например:
- нет ответа менеджера на входящее
- долгий первый ответ
- есть “хочу/цена/наличие”, но нет оффера/следующего шага
- нет уточняющих вопросов

Если хотите — можно добавить LLM-советы (нужен ключ провайдера и согласование по данным).


