# ТЗ: выгрузка диалогов из RetailCRM («Чаты») для оценки менеджеров и роста конверсии

> Связано: [Архитектура](Архитектура.md) | [Бэклог](Бэклог.md) | [INSTINTO](../../{instinto} {spec} Индекс.md)

---

## Цель

Собрать переписки из каналов **WhatsApp** и **Instagram** из папки **«Чаты»** в RetailCRM за период **2–3 месяца**, чтобы:
- оценить работу менеджеров (скорость реакции, потери, follow-up),
- дать **советы по каждому диалогу**,
- сделать **сводный анализ по каждому менеджеру** и по чатам в целом,
- выгрузить всё в **Google Sheets**.

## Источники данных

- RetailCRM → раздел **«Чаты»**
- Каналы: **WhatsApp**, **Instagram**
- Аналитика строится **по менеджерам**.

## Выходные данные (Google Sheets)

### Лист `chats_raw` (1 строка = 1 чат)

Минимальные колонки:
- `chat_id`
- `channel`
- `manager_id`, `manager_name`
- `client_id`
- `order_id` (если чат привязан)
- `created_at`, `updated_at`
- `status`
- `inbound_count`, `outbound_count`
- `first_response_sec`
- `unanswered_inbound`

### Лист `messages_raw` (1 строка = 1 сообщение)

- `chat_id`
- `message_id`
- `sent_at`
- `direction` (in/out)
- `manager_id` (для out)
- `text`

### Лист `manager_summary` (сводка по менеджерам)

- `manager_id`, `manager_name`
- `chats`
- `inbound`, `outbound`
- `unanswered_inbound`
- `slow_first_reply_chats`
- `no_reply_chats`
- `responded_chats`
- `median_first_reply_sec`, `p90_first_reply_sec`
- `response_rate`

### Лист `channel_summary` (сводка по каналам)

- `channel`
- `chats`
- `inbound`, `outbound`
- `no_reply_chats`
- `slow_first_reply_chats`
- `responded_chats`
- `median_first_reply_sec`, `p90_first_reply_sec`
- `response_rate`

### Лист `chat_advice` (советы по каждому чату)

- `chat_id`
- `channel`
- `manager_id`, `manager_name`
- `inbound`, `outbound`
- `first_response_sec`
- `unanswered_inbound`
- `advice` (текстовый список рекомендаций)

### Лист `spin_manager_metrics` (SPIN-анализ по менеджерам)

Метрики по методике SPIN-продаж Нила Рэкхема:
- `manager_id`, `manager_name`
- `total_chats`, `total_messages`, `total_questions`
- `avg_questions_per_chat`
- `spin_s_total`, `spin_p_total`, `spin_i_total`, `spin_n_total` (общее количество)
- `spin_s_per_chat`, `spin_p_per_chat`, `spin_i_per_chat`, `spin_n_per_chat` (на чат)
- `s_usage_rate`, `p_usage_rate`, `i_usage_rate`, `n_usage_rate` (% чатов с использованием этапа)
- `avg_spin_completeness` (средняя полнота SPIN-цикла, %)
- `full_spin_chats`, `full_spin_rate` (количество и % чатов с полным циклом)

### Лист `spin_chat_metrics` (SPIN-анализ по чатам)

- `chat_id`, `manager_id`, `manager_name`
- `total_messages`, `total_questions`
- `spin_s_count`, `spin_p_count`, `spin_i_count`, `spin_n_count`
- `has_situation`, `has_problem`, `has_implication`, `has_need_payoff`
- `spin_completeness` (полнота SPIN-цикла для чата, %)

### Лист `spin_improvement_plans` (планы улучшения)

- `manager_name`
- `improvement_plan` (детальный план улучшения по этапам SPIN)

## Правила расчёта (MVP)

- **Время первого ответа**: разница между первым входящим и первым исходящим.
- **Потерянные входящие**: входящие после последнего исходящего (грубая эвристика).
- **Slow first reply**: первый ответ > 10 минут.

## Правила “советов” (MVP, rule-based)

- Нет исходящих при наличии входящих → “нет ответа менеджера”.
- Первый ответ > 10 минут → “сократить время реакции”.
- Есть входящие после последнего исходящего → “follow-up + зафиксировать следующий шаг”.
- В первых исходящих нет вопроса → “добавить уточняющие вопросы”.
- Клиент спрашивает цену/наличие/покупку, но в ответах нет следующего шага → “дать next step: ссылка/оформление/доставка/оплата”.

## Зафиксированные решения

- Каналы: WhatsApp + Instagram.
- Источник: папка «Чаты» в RetailCRM.
- Аналитика: по менеджерам.
- Формат выгрузки: Google Sheets.
- Режим: разовая выгрузка за 2–3 месяца (с возможностью позже сделать регулярную).
- Рабочие часы для оценки скорости ответа: **10:00–23:00**.
- SPIN-анализ: добавлен анализ по методике SPIN-продаж Нила Рэкхема (4 этапа: S-P-I-N).
- Скрипт SPIN-анализа: `spin_analysis.py` — анализирует переписки по этапам SPIN, сравнивает менеджеров, создаёт планы улучшения.
- План улучшения: документ `План_улучшения_SPIN.md` с детальными шагами для каждого менеджера.

## Открытые вопросы (нужно уточнить перед финальной настройкой API)

1) `RETAILCRM_URL` (домен) и есть ли ограничения IP/доступа к API?
2) Какой модуль подключает WhatsApp/Instagram (native / I2CRM / другое)? Это влияет на API путей.
3) Часовой пояс и “рабочие часы” для корректной оценки скорости реакции (например, 10:00–20:00).


