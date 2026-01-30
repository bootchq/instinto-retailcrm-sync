"""
Единое место для путей API по чатам.

Почему так: в RetailCRM раздел «Чаты» может обслуживаться разными модулями/интеграциями,
и в некоторых аккаунтах пути/форматы могут отличаться.

Если получите 404/403 — это первое место, которое нужно проверить/подправить.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetailCrmEndpoints:
    """
    Базовые REST-эндпоинты.

    Значения ниже — 'разумные дефолты'. Их может потребоваться подстроить под ваш аккаунт.
    """

    # Пример: /api/v5/users
    users: str = "/api/v5/users"

    # --- Чаты (ВОЗМОЖНО потребуется изменить) ---
    # Список чатов за период/с фильтрами
    chats: str = "/api/v5/chats"

    # Сообщения конкретного чата
    chat_messages: str = "/api/v5/chats/{chat_id}/messages"


DEFAULT_ENDPOINTS = RetailCrmEndpoints()


