# Руководство по запуску бенчмарков

Документ описывает запуск скрипта `run_becnhs.py` для сборки проекта, прогона benchmark-сюит и генерации итогового `HTML`-отчёта.

## Системные требования

Нужно установить:
1. **Python** 3.8+
2. **CMake** 3.15+
3. C++ компилятор с поддержкой **C++20**
4. Доступ к **Google Benchmark** через CMake

---

## Режимы запуска

### Стандартный жёсткий прогон (по умолчанию)

Запускает 6 сюит:
- throughput SPSC
- contention MPSC
- contention MPMC
- overwrite pressure
- latency profiles
- capacity sweep

```bash
python bin/run_becnhs.py
```

### Максимально полный прогон

Флаг `--max` добавляет полную матрицу `strategy × profile × topology × capacity`:
- 5 стратегий
- 6 профилей
- 4 топологии
- 3 ёмкости
- итого: **360 кейсов** в `99_full_matrix.json`

```bash
python bin/run_becnhs.py --max
```

Для smoke-проверки:

```bash
python bin/run_becnhs.py --max --repetitions=1
```

---

## Аргументы

| Параметр | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `--max` | флаг | выключен | Добавляет полный прогон матрицы (360 комбинаций) к стандартным 6 сюитам. |
| `--all-threads` | флаг | выключен | Принудительно ставит `thread-ratio=1.0`. |
| `--thread-ratio` | float | `1.0` | Доля доступных аппаратных потоков (`0.1..1.0`). |
| `--repetitions` | int | `7` | Количество повторений каждого benchmark-кейса. |
| `--march-native` | флаг | выключен | Включает native-оптимизации компилятора (`-march=native` для GCC/Clang, `/arch:AVX2` для MSVC). |

---

## Нагрузочные параметры сюит

| Сюита | Messages | Timeout | Backoff |
| :--- | :---: | :---: | :---: |
| `01_throughput_spsc` | 500k | 120s | off |
| `02_contention_mpsc` | 500k | 180s | off |
| `03_contention_mpmc` | 500k | 180s | off |
| `04_overwrite_pressure` | 500k | 180s | off |
| `05_latency_profiles` | 200k | 180s | on |
| `06_capacity_sweep` | 500k | 120s | off |
| `99_full_matrix` (`--max`) | 100k | 600s | on |

---

## Метрики latency

В отчёт публикуются:
- `send_p50_ns`
- `send_p99_ns`
- `read_p50_ns`
- `read_p99_ns`

Хвостовые метрики переведены с `p95` на `p99` для более показательной оценки редких всплесков задержки.

---

## Workflow

Скрипт выполняет:
1. Конфигурацию CMake в `Release` (с учётом `MESSAGE_QUEUE_MARCH_NATIVE`)
2. Сборку `message_queue_benchmark`
3. Запуск набора benchmark-сюит и сохранение `JSON`
4. Генерацию `build/benchmark_results/report.html` через `benchmark_report.py`

---

## Возможные проблемы

### Длительный runtime при `--max`
- **Причина:** Полная матрица (360 кейсов × repetitions) может выполняться часами.
- **Решение:** Для быстрой проверки запускайте `--max --repetitions=1`.

### Ошибка `CMake configuration failed`
- **Причина:** CMake не установлен или не в `PATH`.
- **Решение:** Проверить `cmake --version`.

### Ошибка `Benchmark build failed`
- **Причина:** Ошибки компиляции проекта или окружения.
- **Решение:** Локализовать проблемный таргет через `cmake --build build --config Release --target message_queue_benchmark`.