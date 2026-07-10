# AgentOS — Cognitive Runtime Platform

> Операционная среда для искусственного интеллекта.
> Модельно-независимая, событийно-ориентированная, расширяемая платформа
> для построения интеллектуальных систем любого масштаба.

**Статус проекта:** поэтапная реализация эталонной версии (Python, modular monolith).

---

## Выбор названия

Рабочее название проекта — **AgentOS**, с формальным подзаголовком
*Cognitive Runtime Platform (CRP)*.

Обоснование выбора:

1. **Метафора операционной системы точно отражает архитектуру.**
   Платформа построена по принципу Microkernel: минимальное ядро,
   независимые Runtime-подсистемы (аналог драйверов и системных сервисов),
   планировщик, управление ресурсами, шина сообщений, изоляция и права
   доступа. Это буквально ОС, где «процессами» являются агенты и задачи,
   а «устройствами» — модели, инструменты и хранилища.

2. **Агент — универсальная единица исполнения.**
   Персональный ассистент, корпоративный AI-сервис, компонент IDE или
   участник мультиагентной среды — всё это частные случаи агента.
   Название фиксирует главную абстракцию платформы, как «Kubernetes»
   фиксирует контейнер.

3. **Модельная независимость заложена в имени.**
   «AgentOS», в отличие от «AI Runtime Platform», не привязывает платформу
   к текущему поколению технологий (LLM). Языковая модель — сменяемый
   вычислительный модуль, «процессор», который ОС абстрагирует.

4. **CRP сохраняется как формальный подзаголовок** — он полезен в
   технической документации и точно описывает класс системы, но слишком
   абстрактен как бренд.

---

## Документация

| Документ | Содержание |
|---|---|
| [00 — Обзор архитектуры](docs/architecture/00-overview.md) | Миссия, принципы, слои, карта Runtime-модулей, ключевые диаграммы |
| [01 — Core Runtime](docs/architecture/01-core-runtime.md) | Микроядро: жизненный цикл, DI, реестр сервисов, маршрутизация |
| [02 — Когнитивный слой](docs/architecture/02-cognitive-layer.md) | Session, Context, Memory, Knowledge Runtime |
| [03 — Слой исполнения](docs/architecture/03-execution-layer.md) | Task, Workflow, Scheduler, Event Runtime |
| [04 — Слой интеллекта](docs/architecture/04-intelligence-layer.md) | Agent, Model, Inference, Tool Runtime |
| [05 — Слой управления](docs/architecture/05-governance-layer.md) | Policy, Security, Resource Runtime |
| [06 — Платформенные сервисы](docs/architecture/06-platform-services.md) | Storage, Configuration, Observability, Metrics, Diagnostics Runtime |
| [07 — Интерфейсный слой](docs/architecture/07-interface-layer.md) | API, Plugin, Extension Runtime, Runtime Dashboard |
| [08 — Масштабируемость](docs/architecture/08-scalability.md) | Режимы развёртывания: от single-user до кластера |
| [09 — Безопасность](docs/architecture/09-security.md) | Модель угроз, рекомендации по безопасности |
| [10 — Тестирование](docs/architecture/10-testing.md) | Стратегия и рекомендации по тестированию |
| [11 — Руководство разработчика](docs/architecture/11-developer-guide.md) | Соглашения, структура кода, создание расширений |

---

## Быстрый старт

Эталонная реализация — Python ≥ 3.11, без внешних зависимостей
(pytest — только для тестов).

```bash
# запустить платформу с API и Dashboard (http://127.0.0.1:8080/)
python -m agentos serve --backend sqlite --data ./data

# проверить здоровье и пообщаться с агентом
python -m agentos health
python -m agentos chat assistant "привет"

# тесты (82: unit, contract, интеграционные, E2E через HTTP, кластер)
pip install pytest pytest-asyncio && python -m pytest -q
```

Встраивание в свой процесс (embedded SDK):

```python
from agentos.platform import AgentOSPlatform
from agentos.contracts.agent import AgentDefinition

platform = AgentOSPlatform(models=[...])   # любые адаптеры ModelPort
await platform.start()
platform.agents.register(AgentDefinition(id="assistant"))
reply = await platform.agents.send("assistant", "привет")
```

Структура кода: `agentos/kernel` — микроядро; `agentos/contracts` —
публичные контракты (порты, события, DTO); `agentos/runtimes/*` —
17 Runtime-модулей; `agentos/adapters` — встроенные адаптеры хранилищ и
моделей; `agentos/cluster` — федерация шин, consistent hashing, шлюз
распределённых агентов; `agentos/sdk` — HTTP-клиент.

## Ключевые принципы

- **Microkernel + Modular Monolith → Microservices.** Ядро минимально;
  вся функциональность — в заменяемых Runtime-модулях, которые могут
  выноситься в отдельные процессы/сервисы без изменения контрактов.
- **Event Driven.** Модули взаимодействуют исключительно через события
  и шину сообщений; прямые вызовы допустимы только через
  зарегистрированные интерфейсы портов.
- **Interface First / API First.** Сначала контракт, потом реализация.
  Любой модуль заменяется по интерфейсу без изменения остальной системы.
- **Модельная независимость.** Любая модель (локальная, облачная,
  мультимодальная) — это адаптер за единым интерфейсом `ModelPort`.
- **Всё — расширение.** Модели, инструменты, хранилища, коннекторы,
  политики подключаются как плагины с манифестами.
- **Безопасность по умолчанию.** Sandbox, permissions, политики и аудит —
  сквозные механизмы, а не надстройка.

## Методология разработки

Разработка ведётся строго поэтапно. Каждый этап включает: проектирование,
архитектурный анализ, проверку масштабируемости, безопасности и
расширяемости, подготовку документации. Переход к следующему этапу —
только после подтверждения пользователем.

| Этап | Содержание | Статус |
|---|---|---|
| 1 | Архитектурное проектирование и документация | ✅ Завершён |
| 2 | Core Runtime + Event Runtime (ядро и шина) | ✅ Завершён |
| 3 | Model / Inference / Tool Runtime | ✅ Завершён |
| 4 | Context / Memory / Knowledge Runtime | ✅ Завершён |
| 5 | Agent / Task / Workflow / Scheduler Runtime | ✅ Завершён |
| 6 | Policy / Security / Resource Runtime | ✅ Завершён |
| 7 | API Runtime, SDK, Dashboard | ✅ Завершён |
| 8 | Кластеризация и распределённое исполнение | ✅ Завершён |
