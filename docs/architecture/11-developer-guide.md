# 11 — Руководство разработчика

## 1. Структура репозитория (целевая)

```
agentos/
  kernel/                  # Core Runtime (микроядро)
  contracts/               # Все публичные контракты (порты, события, схемы)
    core/  context/  memory/  knowledge/  ...
  runtimes/
    event/  session/  context/  memory/  knowledge/
    task/  workflow/  scheduler/
    agent/  model/  inference/  tool/
    policy/  security/  resource/
    storage/  configuration/  observability/  metrics/  diagnostics/
    api/  plugin/  extension/
  adapters/                # Встроенные адаптеры (sqlite, in-memory, ...)
  plugins/                 # Плагины первой стороны (провайдеры, инструменты)
  sdk/
    python/  javascript/  plugin-sdk/
  dashboard/               # Веб-панель (потребитель публичного API)
  cli/
  docs/
  tools/                   # Кодогенерация из контрактов, линтеры схем
```

## 2. Правила зависимостей (проверяются линтером сборки)

1. `kernel` не зависит ни от чего, кроме stdlib.
2. `runtimes/*` зависят только от `kernel` (контракт модуля) и
   `contracts/*`. Импорт из чужого `runtimes/*` — ошибка сборки.
3. `contracts/*` — без зависимостей на реализацию, только типы и схемы.
4. `adapters/*` и `plugins/*` зависят от контрактов, не от рантаймов.
5. Домены внутри модуля не импортируют инфраструктуру
   (Clean Architecture: domain ← application ← adapters).

## 3. Как создать новый Runtime-модуль

1. Спроектировать контракт: порты + события в `contracts/<name>/`,
   пройти ревью контракта (Interface First).
2. Сгенерировать каркас: `agentos-dev scaffold runtime <name>`.
3. Реализовать домен и use cases; подключить входящие/исходящие адаптеры.
4. Заполнить `module.manifest.yaml` (provides/requires/permissions —
   минимально необходимые).
5. Поставить test-kit контрактов и пройти Definition of Done
   ([10 — Тестирование](10-testing.md)).

## 4. Как добавить провайдера модели

1. Новый плагин: `agentos-dev scaffold plugin model-adapter <provider>`.
2. Реализовать `ModelPort` (generate/embed/descriptor/health) поверх API
   провайдера; заполнить `ModelDescriptor` честно (лимиты, стоимость,
   capabilities).
3. Прогнать contract-тесты `ModelPort` из test-kit (включая стриминг,
   отмену, ошибки, rate limit).
4. Опубликовать в реестр плагинов; модель становится доступной через
   `ModelRegistry.select()` без изменения какого-либо кода платформы.

## 5. Как добавить инструмент

1. `agentos-dev scaffold plugin tool <name>`.
2. Описать манифест: схемы входа/выхода, permissions, sandbox-уровень,
   лимиты (см. [04 — Слой интеллекта](04-intelligence-layer.md)).
3. Реализовать обработчик; помнить: вход уже валидирован, выход будет
   валидирован; секреты запрашивать через lease, не хранить.
4. Adversarial-тесты: инструмент с враждебным входом от «модели».

## 6. Соглашения

- **События:** имена `domain.entity.action`, payload по
  зарегистрированной схеме, обработчики идемпотентны.
- **Ошибки:** типизированные; категория retryable/terminal обязательна.
- **Версионирование:** semver на модулях, портах, событиях, API, KB.
  Ломающее изменение = новая major-версия контракта, старая живёт до
  окончания deprecation-периода.
- **Логи/трассы:** через контекст модуля (`ctx.logger`, `ctx.tracer`) —
  корреляция и редакция секретов бесплатны.
- **Конфигурация:** только через схему в манифесте; никакого чтения env
  внутри модуля.
- **Никаких синглтонов и глобального состояния** — всё через DI ядра.

## 7. Процесс изменений архитектуры

Существенные решения фиксируются как ADR (Architecture Decision Records)
в `docs/adr/NNN-title.md`: контекст → решение → последствия. Изменение
контракта из `contracts/*` требует ADR и ревью владельцев зависимых
модулей.

## 8. Дорожная карта реализации

Поэтапный план (каждый этап: проектирование → архитектурный анализ →
проверка масштабируемости → безопасности → расширяемости → документация
→ **подтверждение пользователя**):

| Этап | Состав | Результат |
|---|---|---|
| 1 | Архитектура и документация | Этот комплект документов |
| 2 | kernel + contracts/core + Event Runtime + встроенные адаптеры Storage | Запускаемое ядро с шиной и модульным жизненным циклом |
| 3 | Model + Inference + Tool Runtime, 1–2 адаптера моделей | Первый сквозной инференс и вызов инструмента |
| 4 | Session + Context + Memory + Knowledge Runtime | Диалог с памятью и RAG |
| 5 | Agent + Task + Workflow + Scheduler Runtime | Автономные агенты и оркестрация |
| 6 | Policy + Security + Resource Runtime (полные) | Продакшн-безопасность и бюджеты |
| 7 | API Runtime (REST/gRPC/WS) + Python/JS SDK + Dashboard | Публичная поверхность платформы |
| 8 | Кластерные транспорты, шардирование акторов, автоскейлинг | Распределённый режим T3/T4 |
