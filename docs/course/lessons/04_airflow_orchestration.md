# Урок 4. Оркестрация в Airflow

> Формат: **практика** — будешь сам запускать команды и менять код, не только читать.
> Пререквизит: пройден урок 3 (слой DDS собран, ты знаешь, что такое событие-сирота и как
> посчитать `orphan_events`).
> Эталонный путь: [`airflow/dags/etl_pipeline_dag.py`](../../../airflow/dags/etl_pipeline_dag.py).
>
> Поток данных одной строкой:
> `precheck → transform: wait → ods → dq → branch → dds → integrity → dm → validate`
>
> О чём урок простыми словами: собираем все шаги STG → ODS → DDS → DM в один управляемый
> сценарий Airflow. И превращаем счётчик сирот из прошлого урока в стоп-кран: если событие
> ссылается на несуществующий клик, пайплайн краснеет и не идёт дальше.

---

## 1. Зачем и где в проде

В прошлых уроках мы смотрели на слои по отдельности: STG принял поток, ODS разобрал JSON, DDS
собрал сущности. Но в проде эти шаги не запускают «по памяти» руками. Иначе легко ошибиться:
сначала собрать DDS до ODS, забыть проверку, не заметить пустую витрину или пропустить сироту.

Поэтому появляется **оркестрация** — управление порядком работ. В нашем стенде этим занимается
Airflow. Главная единица Airflow — **DAG** (Directed Acyclic Graph, направленный ациклический
граф). Проще: это схема задач без петли назад. В ней видно:

- какие шаги есть в пайплайне;
- какой шаг ждёт какой;
- где пайплайн должен остановиться, если данные плохие;
- какой именно шаг покраснел, когда что-то сломалось.

Отдельный шаг в Airflow называется **task** («задача»). Например, `load_ods` — одна task:
выполнить SQL для слоя ODS. `check_dds_integrity` — другая task: посчитать события-сироты.
А весь `etl_pipeline` — DAG, который связывает эти задачи в правильном порядке.

### Проверка и гейт — не одно и то же

Важная мысль урока: не каждая проверка должна ронять пайплайн.

Есть проверки, которые **измеряют**. Например, `check_ods_quality` считает, сколько строк с
`parse_errors` появилось в ODS. Это полезная метрика: её можно отправить в лог, XCom или мониторинг.
Но сам факт ошибки парсинга в нашем учебном стенде не блокирует весь прогон. Грязные записи уже
отложены в `ods.*_errors`, чистые продолжают ехать дальше.

А есть проверки, которые **гейтят**. **Гейт** (gate, «ворота») — это проверка, через которую
данные должны пройти, иначе следующие шаги не запускаются. Сироты в DDS — как раз такой случай.
Если `dds.event.click_id` ссылается на клик, которого нет в `dds.click`, целостность связей
сломана. В уроке 3 мы это только считали. Теперь DAG сам скажет: `orphan_events > 0`, дальше
не идём.

> **В проде иначе.** Правил-гейтов обычно больше: свежесть данных, объём относительно вчера,
> доля ошибок, обязательные справочники. Но принцип тот же: одни проверки только измеряют, другие
> останавливают выпуск данных дальше.

---

## 2. Руки: запускаем DAG и смотрим зелёный прогон

Подними стенд, создай схему и залей малый срез:

```bash
make up
make ddl
LIMIT=50 make data
```

Открой Airflow: `http://localhost:8080` (логин `admin`, пароль `admin`). Найди DAG
`etl_pipeline` и запусти его через **Trigger DAG with config**:

```json
{"full_refresh": true}
```

`full_refresh=true` значит: перед сборкой DDS Airflow очистит `dds.click` и `dds.event`, а потом
заново наполнит их из ODS. Для учебного стенда это удобный чистый прогон: результат повторяемый,
старые эксперименты не мешают.

Когда DAG завершится, открой его граф. На чистом срезе все задачи должны быть зелёными. Найди
внутри группы `transform` две задачи подряд:

- `check_dds_integrity` — SQL-задача, которая считает сирот;
- `assert_dds_integrity` — Python-задача, которая решает, можно ли идти дальше.

На чистом срезе `assert_dds_integrity` зелёная: сирот нет, пайплайн прошёл в DM.

Проверь то же число в ClickHouse play-консоли `http://localhost:9123/play`:

```sql
SELECT check_date, layer, table_name, check_name, check_value
FROM dm.dq_summary
WHERE layer = 'dds'
  AND table_name = 'event_without_click'
  AND check_name = 'orphan_events';
```

Ожидаем `check_value = 0`. Это тот же смысл, что в уроке 3, только теперь число появилось внутри
управляемого прогона Airflow.

---

## 3. Загляни внутрь

Открой [`airflow/dags/etl_pipeline_dag.py`](../../../airflow/dags/etl_pipeline_dag.py). Сначала
прочитай верхний docstring. Там есть короткая карта задач:

```text
precheck -> transform: wait -> ods -> dq -> branch -> dds -> integrity -> dm -> validate
```

Это не SQL-слои, а именно задачи DAG:

- **`precheck`** — проверить, что ClickHouse доступен и схема уже создана;
- **`wait`** — дождаться строк в STG, чтобы не пересчитывать пустоту;
- **`ods`** — выполнить батч STG → ODS;
- **`dq`** — измерить качество ODS;
- **`branch`** — выбрать, чистить ли DDS перед загрузкой;
- **`dds`** — собрать `dds.click` и `dds.event`;
- **`integrity`** — проверить целостность событий и кликов;
- **`dm`** — собрать сводку качества `dm.dq_summary`;
- **`validate`** — убедиться, что сводка не пустая.

Дальше разберём пять мест в файле.

### `ClickHouseOperator`: SQL как отдельная задача

Большая часть DAG — это задачи на `ClickHouseOperator`. Они выполняют SQL в ClickHouse:

```python
load_ods = ClickHouseOperator(
    task_id="load_ods",
    sql=load_sql_statements("ods/20_stg_to_ods.sql"),
    clickhouse_conn_id="clickhouse_default",
    database="default",
)
```

Это читается почти как команда: задача `load_ods` берёт SQL-файл `ods/20_stg_to_ods.sql` и
выполняет его в ClickHouse. Так же устроены `load_dds`, `load_dm_summary` и SQL-проверки.

### `PythonOperator`: управляющая логика на Python

**`PythonOperator`** — оператор Airflow, который запускает обычную Python-функцию как task.
В этом DAG Python нужен не для трансформации данных, а для управляющих решений:

- `wait_for_stg_data` ждёт, пока в STG появятся строки;
- `assert_schema_ready` падает, если DDL не применён;
- `assert_dds_integrity` падает, если есть сироты;
- `assert_dm_summary_not_empty` падает, если финальная сводка пустая.

То есть данные мы меняем SQL-ем, а Python оставляем для «можно ли продолжать».

### `XCom`: маленькая передача результата между task

Airflow хранит маленькие результаты задач в **XCom** (cross-communication, «передача между
задачами»). Это не место для данных пайплайна: туда не кладут таблицы и большие JSON. Но туда
нормально положить маленький результат проверки.

Так работает пара `check_dds_integrity` → `assert_dds_integrity`:

```python
check_dds_integrity = ClickHouseOperator(
    task_id="check_dds_integrity",
    sql=SQL_CHECK_DDS_INTEGRITY,
    ...
)

assert_dds_integrity_task = PythonOperator(
    task_id="assert_dds_integrity",
    python_callable=assert_dds_integrity,
    retries=0,
)
```

SQL-задача возвращает одну строку с одним числом — `orphan_events`. Python-задача достаёт это
число из XCom:

```python
result = ti.xcom_pull(task_ids="transform.check_dds_integrity")
```

Если число больше нуля, функция бросает `AirflowException`. Для Airflow это значит: task упала.
А у следующих задач по умолчанию правило «запускайся только если upstream успешен», поэтому
`load_dm_summary` дальше не пойдёт.

У этой задачи отдельно стоит `retries=0`: если целостность уже нарушена, повтор через две минуты
ничего не исправит. Для учебного гейта честнее сразу показать красную задачу.

> **Что проверили по API.** Перед правкой мы сверили Airflow через MCP Context7: `PythonOperator`
> подходит для Python-проверки, результат upstream можно читать через `ti.xcom_pull`, зависимости
> задаются оператором `>>`, а `AirflowException` переводит task в ошибку. Для ветвления после
> `BranchPythonOperator` оставлен `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`, чтобы join не
> пропускался из-за skipped-ветки.

### `BranchPythonOperator`: развилка full refresh

**Branching** («ветвление») — это выбор одной из нескольких веток DAG. У нас развилка простая:
чистить DDS перед загрузкой или не чистить.

```python
choose_refresh_mode = BranchPythonOperator(
    task_id="choose_refresh_mode",
    python_callable=choose_full_refresh,
)
```

Если `full_refresh=true`, функция выбирает `transform.truncate_dds_click`: DAG очищает
`dds.click`, потом `dds.event`, и только после этого грузит DDS заново. Если `full_refresh=false`,
Airflow идёт через `skip_truncate` и сохраняет текущие строки DDS.

После развилки ветки снова сходятся в `truncate_complete`. У этой задачи стоит специальное
правило `NONE_FAILED_MIN_ONE_SUCCESS`: «ни одна выбранная ветка не упала, и хотя бы одна успешно
прошла». Без него Airflow мог бы считать skipped-ветку проблемой.

### Цепочка `>>`: порядок выполнения

В самом низу файла порядок задач задан стрелками `>>`:

```python
truncate_complete >> load_dds >> check_dds_integrity >> assert_dds_integrity_task
```

Читается слева направо:

1. дождаться завершения развилки;
2. собрать DDS;
3. посчитать сирот;
4. проверить число и, если надо, уронить DAG.

И только после этого идут `load_dm_summary`, `validate_dm_summary_sql` и `validate_dm_summary`.
Так гейт стоит именно там, где нужен: после сборки DDS, но до выпуска DM.

---

## 4. Управляемая правка: заведём сироту и уроним DAG

Сейчас воспроизведём обещание из урока 3: заведём событие-сироту и увидим, как Airflow красит
конкретную задачу.

### Шаг 1. Вставь сироту в DDS

Открой ClickHouse play-консоль `http://localhost:9123/play` и выполни:

```sql
-- Событие со ссылкой на несуществующий клик dddd...-dddd (такого в dds.click нет)
INSERT INTO dds.event (event_id, event_ts, event_type, click_id, browser_name, dds_update_ts, ods_parse_errors)
VALUES (
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    now64(6), 'pageview',
    'dddddddd-dddd-dddd-dddd-dddddddddddd',
    'DemoBrowser', now64(3), []
);
```

Проверь, что сирота правда появилась:

```sql
SELECT count() AS orphans
FROM dds.event
WHERE click_id IS NOT NULL
  AND click_id NOT IN (SELECT click_id FROM dds.click);
```

Ожидаем `orphans = 1`.

### Шаг 2. Запусти DAG без очистки DDS

Теперь вернись в Airflow и запусти `etl_pipeline` через **Trigger DAG with config**:

```json
{"full_refresh": false}
```

Здесь важен именно `false`. Если поставить `true`, DAG сначала очистит `dds.event`, и наша
ручная сирота исчезнет ещё до проверки. А с `full_refresh=false` мы специально сохраняем текущий
DDS и даём гейту поймать битую связь.

В графе Airflow ожидаем такую картину:

- `check_dds_integrity` зелёная — SQL успешно посчитал `orphan_events`;
- `assert_dds_integrity` красная — Python-проверка увидела `orphan_events = 1` и бросила ошибку;
- `load_dm_summary` и последующие задачи дальше не пошли.

Открой лог `assert_dds_integrity`. В нём должно быть сообщение примерно такого смысла:

```text
DDS integrity check failed: orphan_events=1. Есть события, чей click_id отсутствует в dds.click.
```

Вот теперь сирота не просто «видна запросом». Она стала настоящим гейтом пайплайна.

### Верни как было

Чтобы вернуть стенд в чистое состояние, запусти `etl_pipeline` ещё раз, но уже с очисткой DDS:

```json
{"full_refresh": true}
```

После зелёного прогона проверь:

```sql
SELECT count() AS orphans
FROM dds.event
WHERE click_id IS NOT NULL
  AND click_id NOT IN (SELECT click_id FROM dds.click);
```

Снова должно быть `0`. Если стенд после экспериментов совсем запутался, полный сброс остаётся
тем же:

```bash
make clean && make up && make ddl && LIMIT=50 make data
```

После этого запусти `etl_pipeline` с `{"full_refresh": true}`.

---

## 5. Проверь себя

| Действие | Где смотреть | Что ожидать |
|----------|--------------|-------------|
| `etl_pipeline` с `{"full_refresh": true}` | Airflow graph | все задачи зелёные |
| чистый прогон | `dm.dq_summary`, строка `orphan_events` | `0` |
| вставка события-сироты | прямой SQL-счётчик сирот | `0 → 1` |
| `etl_pipeline` с `{"full_refresh": false}` после вставки | task `transform.assert_dds_integrity` | task красная, DAG failed |
| откат через `{"full_refresh": true}` | прямой SQL-счётчик сирот | снова `0` |

---

## 6. Что должно получиться

После урока у тебя на руках — видимый результат:

- скрин Airflow graph, где `transform.assert_dds_integrity` красная после вставки сироты;
- и рядом короткое объяснение своими словами: почему `check_dds_integrity` зелёная, а
  `assert_dds_integrity` красная.

Проверь себя на словах — примерно эти вопросы всплывут на еженедельном созвоне:

- что такое DAG и task в Airflow;
- чем проверка-метрика отличается от проверки-гейта;
- почему `check_ods_quality` только измеряет, а `assert_dds_integrity` останавливает пайплайн;
- зачем в управляемой правке нужен `full_refresh=false`;
- почему гейт стоит после `load_dds`, но до `load_dm_summary`.

Если ответ на последний вопрос получается мутным, вернись к цепочке внизу DAG: DM должна
собираться только из DDS, который уже прошёл проверку целостности.

---

## Вся цепочка разом: STG → ODS → DDS → DM

Теперь у нас есть и сами слои, и порядок их жизни:

- **STG** принимает поток и хранит сырой JSON;
- **ODS** типизирует и разделяет чистое/битое;
- **DDS** собирает сущности и проверяет связи между ними;
- **DM** даёт готовые витрины и сводку качества;
- **Airflow** связывает всё это в DAG, где виден порядок, статус и место падения.

Главное изменение этого урока — сироты перестали быть ручным запросом «когда-нибудь посмотреть».
Теперь это автоматический гейт: если связь `dds.event → dds.click` порвалась, DAG показывает
красную задачу и не выпускает следующие шаги.

---

## Мост к уроку 5

Airflow хорошо показывает судьбу конкретного прогона: зелёный он или красный, на какой задаче
упал, что написано в логе. Но есть другой вопрос: как увидеть состояние всего стенда со стороны —
живы ли сервисы, есть ли лаг в Kafka, не пропали ли метрики качества? В уроке 5 перейдём к
мониторингу: Prometheus и Grafana покажут пайплайн не как один DAG-run, а как систему, за которой
можно наблюдать постоянно.
