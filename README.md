# Rusty Chat Android Client

Исправленная версия с рабочими методами доступа к свойствам Slint.

## Изменения
- Использованы `visible` вместо `if` для переключения экранов (теперь геттеры/сеттеры публичны)
- Убран `stream_holder` и `try_clone()`, используется `into_split()`
- Сохранена вся функциональность

## Сборка
```bash
cargo apk build --release
cargo apk run
