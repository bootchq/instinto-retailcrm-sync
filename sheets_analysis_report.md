# Анализ структуры Google Sheets

> Связано: [ТЗ](ТЗ_выгрузка_чатов_RetailCRM.md) | [Архитектура](Архитектура.md) | [Бэклог](Бэклог.md) | 

---


Всего листов: 21

## Лист1

- Строк данных: 0
- Колонок: 0
- Колонки: 

## chats_raw

- Строк данных: 7562
- Колонок: 17
- Колонки: , , , , , , , , , , , , , , , , 

## messages_raw

- Строк данных: 128422
- Колонок: 8
- Колонки: , , , , , , , 

## manager_summary

- Строк данных: 1
- Колонок: 12
- Колонки: manager_id, manager_name, chats, inbound, outbound, unanswered_inbound, slow_first_reply_chats, no_reply_chats, responded_chats, median_first_reply_sec, p90_first_reply_sec, response_rate

## channel_summary

- Строк данных: 1
- Колонок: 10
- Колонки: channel, chats, inbound, outbound, no_reply_chats, slow_first_reply_chats, responded_chats, median_first_reply_sec, p90_first_reply_sec, response_rate

## chat_advice

- Строк данных: 5
- Колонок: 9
- Колонки: chat_id, channel, manager_id, manager_name, inbound, outbound, first_response_sec, unanswered_inbound, advice

## manager_report

- Строк данных: 7
- Колонок: 17
- Колонки: type, name, manager_id, manager_name, chats, inbound, outbound, no_reply_chats, no_reply_rate, unanswered_inbound, unanswered_rate, median_first_reply_min, p90_first_reply_min, out_in_ratio, response_rate, focus, coaching_focus

## behavior_snapshot_managers

- Строк данных: 3
- Колонок: 16
- Колонки: run_ts, manager_id, manager_name, chats, responded_chats, response_rate, no_reply_chats, no_reply_rate, median_first_reply_sec, p90_first_reply_sec, avg_questions_per_chat, next_step_rate, spin_rate, upsell_rate, follow_up_gap_rate, high_intent_chats

## history_behavior_managers

- Строк данных: 6
- Колонок: 16
- Колонки: run_ts, manager_id, manager_name, chats, responded_chats, response_rate, no_reply_chats, no_reply_rate, median_first_reply_sec, p90_first_reply_sec, avg_questions_per_chat, next_step_rate, spin_rate, upsell_rate, follow_up_gap_rate, high_intent_chats

## weekly_behavior_delta_managers

- Строк данных: 3
- Колонок: 25
- Колонки: run_ts, manager_id, manager_name, chats, responded_chats, response_rate, no_reply_chats, no_reply_rate, median_first_reply_sec, p90_first_reply_sec, avg_questions_per_chat, next_step_rate, spin_rate, upsell_rate, follow_up_gap_rate, high_intent_chats, delta_response_rate, delta_no_reply_rate, delta_avg_questions_per_chat, delta_next_step_rate, delta_spin_rate, delta_upsell_rate, delta_follow_up_gap_rate, delta_median_first_reply_sec, delta_p90_first_reply_sec

## weekly_examples

- Строк данных: 27
- Колонок: 8
- Колонки: run_ts, manager_id, manager_name, category, chat_id, snippet_in, snippet_out, note

## spin_manager_metrics

- Строк данных: 2
- Колонок: 21
- Колонки: manager_id, manager_name, total_chats, total_messages, total_questions, avg_questions_per_chat, spin_s_total, spin_p_total, spin_i_total, spin_n_total, spin_s_per_chat, spin_p_per_chat, spin_i_per_chat, spin_n_per_chat, s_usage_rate, p_usage_rate, i_usage_rate, n_usage_rate, avg_spin_completeness, full_spin_chats, full_spin_rate

## spin_chat_metrics

- Строк данных: 2897
- Колонок: 14
- Колонки: chat_id, manager_id, manager_name, total_messages, total_questions, spin_s_count, spin_p_count, spin_i_count, spin_n_count, has_situation, has_problem, has_implication, has_need_payoff, spin_completeness

## spin_improvement_plans

- Строк данных: 2
- Колонок: 2
- Колонки: manager_name, improvement_plan

## manager_top_problems

- Строк данных: 6
- Колонок: 10
- Колонки: manager_name, manager_id, problem_rank, priority, problem_name, current_value, target_value, impact, description, severity_score

## weekly_metrics_tracking

- Строк данных: 12
- Колонок: 10
- Колонки: manager_name, manager_id, metric_name, current, target_week1, target_week2, target_week4, formula, check_frequency, measurement

## improvement_steps_detailed

- Строк данных: 12
- Колонок: 8
- Колонки: manager_name, manager_id, step_number, description, actions, expected_result, timeframe, success_criteria

## best_chats_analysis

- Строк данных: 0
- Колонок: 11
- Колонки: chat_id, manager_name, has_order, spin_completeness, questions_count, has_situation, has_problem, has_implication, has_need_payoff, key_phrases, sample_messages

## best_scripts

- Строк данных: 13
- Колонок: 4
- Колонки: stage, script, usage_count, spin_stage

## chat_order_payment

- Строк данных: 5
- Колонок: 10
- Колонки: chat_id, manager_name, client_id, order_id, has_order, payment_status, payment_status_ru, is_successful, order_total, order_paid

## manager_order_payment_stats

- Строк данных: 1
- Колонок: 7
- Колонки: manager_name, total_chats, chats_with_order, order_rate, chats_paid, chats_successful, success_rate

