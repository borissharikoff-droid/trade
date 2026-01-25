# Анализ логики БАЛАНСА и ИСТОРИИ в bot.py

## 1. ВСЕ ТОЧКИ ИЗМЕНЕНИЯ БАЛАНСА

### 1.1. Депозиты (увеличение баланса)

#### ✅ С LOCK (безопасно):
- **Строка 2421**: `successful_payment` - депозит через Stars
  - Формула: `user['balance'] += usd`
  - Lock: `async with get_user_lock(user_id)`
  - Сохранение: `save_user(user_id)`

- **Строка 2730**: `process_crypto_payment` - депозит через CryptoBot
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + amount)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - Сохранение: `save_user(user_id)`

- **Строка 2836**: `process_crypto_payment_auto` - авто-депозит
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + amount)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - Сохранение: `save_user(user_id)`

#### ⚠️ Реферальные бонусы (потенциальные race conditions):
- **Строка 1002**: `add_referral_bonus` - прямой SQL UPDATE
  - Формула: `UPDATE users SET balance = balance + ?`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - прямой SQL без lock)
  - Кэш обновляется отдельно: `users_cache[referrer_id]['balance'] = sanitize_balance(...)`

- **Строка 1065**: `process_multilevel_deposit_bonus` - прямой SQL UPDATE
  - Формула: `UPDATE users SET balance = balance + ?`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - прямой SQL без lock)
  - Кэш обновляется отдельно

- **Строка 1850**: `add_commission` - реферальные комиссии
  - Формула: `UPDATE users SET balance = balance + ?`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - прямой SQL без lock)
  - Кэш обновляется отдельно

### 1.2. Выводы (уменьшение баланса)

#### ✅ С LOCK (безопасно):
- **Строка 3109**: `process_withdraw_address` - вывод средств
  - Формула: `user['balance'] = sanitize_balance(user['balance'] - amount)`
  - Lock: `async with get_user_lock(user_id)`
  - Проверка: `if user.get('balance', 0) < amount` перед списанием
  - Сохранение: `save_user(user_id)`

### 1.3. Открытие позиций (списание баланса)

#### ⚠️ БЕЗ LOCK (потенциальные race conditions):
- **Строка 4670-4671**: `open_trade` - открытие позиции
  - Формула: 
    ```python
    user['balance'] -= amount
    user['balance'] = sanitize_balance(user['balance'])
    ```
  - Lock: НЕТ (⚠️ КРИТИЧЕСКАЯ ПРОБЛЕМА)
  - Сохранение: `save_user(user_id)`
  - Проблема: Двойное изменение баланса без lock

- **Строка 5411-5412**: `open_trade` (дубликат функции?)
  - Формула: 
    ```python
    user['balance'] -= amount
    user['balance'] = sanitize_balance(user['balance'])
    ```
  - Lock: НЕТ (⚠️ КРИТИЧЕСКАЯ ПРОБЛЕМА)
  - Сохранение: `save_user(user_id)`

- **Строка 4344-4345**: `send_smart_signal` - авто-торговля
  - Формула:
    ```python
    auto_user['balance'] -= auto_bet
    auto_user['balance'] = sanitize_balance(auto_user['balance'])
    ```
  - Lock: `async with get_user_lock(auto_user_id)` ✅
  - Сохранение: `save_user(auto_user_id)`

### 1.4. Закрытие позиций (возврат баланса + PnL)

#### ⚠️ БЕЗ LOCK (потенциальные race conditions):
- **Строка 3413**: `sync_bybit_positions` - закрытие orphan позиций
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - returned = `pos['amount']` (без PnL для orphan)

- **Строка 3459**: `sync_bybit_positions` - закрытие по Bybit синхронизации
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - returned = `pos['amount'] + real_pnl`
  - total_profit: `user['total_profit'] += real_pnl`

- **Строка 3661**: `close_all_trades` - закрытие всех позиций вручную
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле!)
  - returned = `pos['amount'] + pnl`
  - total_profit: `user['total_profit'] += pnl`
  - Сохранение: `save_user(user_id)` только ОДИН РАЗ после цикла

- **Строка 3803**: `close_all_trades` - закрытие всех позиций
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + total_returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - total_returned накапливается в цикле
  - total_profit: `user['total_profit'] += total_pnl`

- **Строка 5044**: `close_all_symbol_trades` - закрытие всех позиций по символу
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + total_returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА)
  - total_returned накапливается в цикле

#### ✅ С LOCK (безопасно):
- **Строка 4882**: `close_trade` - ручное закрытие одной позиции
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: `async with get_user_lock(user_id)` ✅
  - returned = `amount + pnl`
  - total_profit: `user['total_profit'] = (user.get('total_profit', 0) or 0) + pnl`

#### ⚠️ БЕЗ LOCK (в циклах обновления позиций):
- **Строка 5629**: `process_user_positions` - закрытие по Bybit синхронизации
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле обработки позиций)
  - returned = `pos['amount'] + real_pnl`
  - total_profit: `user['total_profit'] += real_pnl`

- **Строка 5758**: `process_user_positions` - частичное закрытие (25% на полпути к TP)
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле)
  - returned = `partial_close_amount + partial_pnl`
  - total_profit: `user['total_profit'] += partial_pnl`

- **Строка 5927**: `process_user_positions` - ранний выход (early exit)
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле)
  - returned = `close_amount + exit_pnl`
  - total_profit: `user['total_profit'] += exit_pnl`

- **Строка 6014**: `process_user_positions` - закрытие по TP1
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле)
  - returned = `close_amount + partial_pnl`
  - total_profit: `user['total_profit'] += partial_pnl`

- **Строка 6074**: `process_user_positions` - закрытие по TP2
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле)
  - returned = `close_amount + partial_pnl`
  - total_profit: `user['total_profit'] += partial_pnl`

- **Строка 6157**: `process_user_positions` - закрытие по TP3/SL
  - Формула: `user['balance'] = sanitize_balance(user['balance'] + returned)`
  - Lock: НЕТ (⚠️ ПРОБЛЕМА - в цикле)
  - returned = `pos['amount'] + real_pnl`
  - total_profit: `user['total_profit'] += real_pnl`

### 1.5. Служебные функции

- **Строка 1795**: `safe_balance_update` - безопасное обновление баланса
  - Формула: `user['balance'] = sanitize_balance(new_balance)`
  - Lock: `async with lock` ✅
  - Проверка: `if new_balance < 0: return False`
  - Сохранение: `save_user(user_id)`
  - ⚠️ НО: функция определена, но НЕ ИСПОЛЬЗУЕТСЯ в большинстве мест!

## 2. ФУНКЦИЯ sanitize_balance

```python
def sanitize_balance(balance: float) -> float:
    """Ensure balance stays within safe bounds"""
    return max(0.0, min(MAX_BALANCE, balance))
```

**Проблемы:**
- ✅ Защищает от отрицательных значений
- ✅ Защищает от превышения MAX_BALANCE (1,000,000)
- ⚠️ НО: НЕ защищает от race conditions
- ⚠️ НО: НЕ проверяет баланс ПЕРЕД изменением (только после)

## 3. ФУНКЦИЯ db_close_position

```python
def db_close_position(pos_id: int, exit_price: float, pnl: float, reason: str):
    """Закрыть позицию и перенести в историю"""
    # 1. Получаем позицию из БД
    # 2. Вставляем в history
    # 3. Удаляем из positions
    # 4. Записываем результат для статистики
```

**Проблемы:**
- ✅ Корректно переносит позицию в историю
- ✅ Записывает PnL в историю
- ⚠️ НО: НЕ обновляет баланс пользователя (это делается отдельно)
- ⚠️ НО: НЕ обновляет total_profit (это делается отдельно)
- ⚠️ ПРОБЛЕМА: Разделение логики может привести к рассинхронизации

## 4. ОБНОВЛЕНИЯ total_profit

### Места обновления total_profit:

1. **Строка 3460**: `sync_bybit_positions` - `user['total_profit'] += real_pnl`
2. **Строка 3662**: `close_all_trades` - `user['total_profit'] += pnl` (в цикле)
3. **Строка 3804**: `close_all_trades` - `user['total_profit'] += total_pnl`
4. **Строка 4883**: `close_trade` - `user['total_profit'] = (user.get('total_profit', 0) or 0) + pnl`
5. **Строка 5045**: `close_all_symbol_trades` - `user['total_profit'] += total_pnl`
6. **Строка 5630**: `process_user_positions` - `user['total_profit'] += real_pnl` (в цикле)
7. **Строка 5759**: `process_user_positions` - `user['total_profit'] += partial_pnl` (в цикле)
8. **Строка 5928**: `process_user_positions` - `user['total_profit'] += exit_pnl` (в цикле)
9. **Строка 6015**: `process_user_positions` - `user['total_profit'] += partial_pnl` (в цикле)
10. **Строка 6075**: `process_user_positions` - `user['total_profit'] += partial_pnl` (в цикле)
11. **Строка 6158**: `process_user_positions` - `user['total_profit'] += real_pnl` (в цикле)

**Проблемы:**
- ⚠️ total_profit обновляется БЕЗ lock в большинстве мест
- ⚠️ total_profit обновляется в циклах без атомарности
- ⚠️ Есть функция `db_sync_user_profit` для синхронизации, но она не вызывается автоматически
- ⚠️ total_profit может рассинхронизироваться с историей сделок

## 5. ПОТЕНЦИАЛЬНЫЕ RACE CONDITIONS

### 5.1. Критические проблемы (без lock):

1. **Открытие позиций (строки 4670, 5411)**:
   - Двойное изменение баланса без lock
   - Может привести к двойному списанию при параллельных запросах

2. **Закрытие позиций в циклах (строки 3661, 3803, 5044, 5629, 5758, 5927, 6014, 6074, 6157)**:
   - Изменение баланса в циклах без lock
   - При параллельной обработке нескольких позиций может произойти потеря обновлений

3. **Реферальные бонусы (строки 1002, 1065, 1850)**:
   - Прямой SQL UPDATE без lock
   - Кэш обновляется отдельно - может быть рассинхронизация
   - При параллельных депозитах может быть двойное начисление

4. **Депозиты без lock (строки 2730, 2836)**:
   - Изменение баланса без lock
   - При параллельных депозитах может быть потеря обновлений

### 5.2. Проблемы с двойным списанием:

1. **Строка 4670-4671**: Двойное изменение баланса:
   ```python
   user['balance'] -= amount
   user['balance'] = sanitize_balance(user['balance'])
   ```
   - Если между этими строками произойдет другое изменение баланса, оно может быть потеряно

2. **Строка 4344-4345**: Аналогичная проблема (но есть lock)

3. **Строка 5411-5412**: Аналогичная проблема (без lock)

### 5.3. Проблемы с синхронизацией кэша и БД:

1. **Реферальные бонусы**: 
   - SQL UPDATE обновляет БД напрямую
   - Кэш обновляется отдельно
   - Если между обновлениями произойдет `get_user()`, вернется старое значение

2. **Закрытие позиций**:
   - Баланс обновляется в кэше
   - `save_user()` сохраняет в БД
   - Но если между обновлением и сохранением произойдет `get_user()` из другого потока, вернется старое значение

## 6. ФОРМУЛЫ ИЗМЕНЕНИЯ БАЛАНСА

### 6.1. Открытие позиции:
```
new_balance = old_balance - amount
new_balance = sanitize_balance(new_balance)
```

### 6.2. Закрытие позиции:
```
returned = amount + pnl
new_balance = old_balance + returned
new_balance = sanitize_balance(new_balance)
```

Где:
- `amount` - сумма открытой позиции
- `pnl` - прибыль/убыток = `amount * LEVERAGE * pnl_percent - commission`

### 6.3. Частичное закрытие:
```
returned = close_amount + partial_pnl
new_balance = old_balance + returned
new_balance = sanitize_balance(new_balance)
```

Где:
- `close_amount` - часть суммы позиции
- `partial_pnl` - прибыль/убыток от частичного закрытия

### 6.4. Депозит:
```
new_balance = old_balance + deposit_amount
new_balance = sanitize_balance(new_balance)  # не всегда
```

### 6.5. Вывод:
```
new_balance = old_balance - withdraw_amount
new_balance = sanitize_balance(new_balance)
```

## 7. РЕКОМЕНДАЦИИ ПО ИСПРАВЛЕНИЮ

### 7.1. Критические исправления:

1. **Обернуть все изменения баланса в `safe_balance_update()` или lock**:
   - Строки 4670-4671, 5411-5412: добавить lock
   - Строки 2730, 2836: добавить lock
   - Все места в циклах закрытия позиций: обернуть в lock

2. **Исправить реферальные бонусы**:
   - Использовать `safe_balance_update()` вместо прямого SQL
   - Или добавить lock перед SQL UPDATE и обновлением кэша

3. **Исправить двойное изменение баланса**:
   - Заменить двойное изменение на одно:
   ```python
   # Вместо:
   user['balance'] -= amount
   user['balance'] = sanitize_balance(user['balance'])
   
   # Использовать:
   user['balance'] = sanitize_balance(user['balance'] - amount)
   ```

4. **Добавить проверку баланса перед списанием**:
   - Перед открытием позиции проверять: `if user['balance'] < amount + MIN_BALANCE_RESERVE: return False`

### 7.2. Улучшения:

1. **Использовать `safe_balance_update()` везде**:
   - Заменить все прямые изменения баланса на вызов `safe_balance_update()`

2. **Атомарное обновление баланса и total_profit**:
   - Создать функцию `safe_close_position()` которая атомарно:
     - Обновляет баланс
     - Обновляет total_profit
     - Закрывает позицию в БД

3. **Синхронизация total_profit**:
   - Периодически синхронизировать total_profit с историей сделок
   - Или использовать триггеры в БД для автоматической синхронизации

4. **Добавить транзакции**:
   - Использовать транзакции БД для атомарности операций

## 8. СПИСОК ВСЕХ МЕСТ ИЗМЕНЕНИЯ БАЛАНСА

| Строка | Функция | Тип | Lock | Проблема |
|--------|---------|-----|------|----------|
| 1002 | add_referral_bonus | + | ❌ | Прямой SQL, рассинхронизация кэша |
| 1065 | process_multilevel_deposit_bonus | + | ❌ | Прямой SQL, рассинхронизация кэша |
| 1850 | add_commission | + | ❌ | Прямой SQL, рассинхронизация кэша |
| 2421 | successful_payment | + | ✅ | OK |
| 2730 | process_crypto_payment | + | ❌ | Нет lock |
| 2836 | process_crypto_payment_auto | + | ❌ | Нет lock |
| 3109 | process_withdraw_address | - | ✅ | OK |
| 3413 | sync_bybit_positions | + | ❌ | Нет lock |
| 3459 | sync_bybit_positions | + | ❌ | Нет lock |
| 3661 | close_all_trades | + | ❌ | Нет lock, в цикле |
| 3803 | close_all_trades | + | ❌ | Нет lock |
| 4344 | send_smart_signal | - | ✅ | OK |
| 4670 | open_trade | - | ❌ | Нет lock, двойное изменение |
| 4882 | close_trade | + | ✅ | OK |
| 5044 | close_all_symbol_trades | + | ❌ | Нет lock |
| 5411 | open_trade | - | ❌ | Нет lock, двойное изменение |
| 5629 | process_user_positions | + | ❌ | Нет lock, в цикле |
| 5758 | process_user_positions | + | ❌ | Нет lock, в цикле |
| 5927 | process_user_positions | + | ❌ | Нет lock, в цикле |
| 6014 | process_user_positions | + | ❌ | Нет lock, в цикле |
| 6074 | process_user_positions | + | ❌ | Нет lock, в цикле |
| 6157 | process_user_positions | + | ❌ | Нет lock, в цикле |

**Итого:**
- ✅ С lock: 4 места
- ❌ Без lock: 18 мест
- ⚠️ Критические: 6 мест (открытие позиций, реферальные бонусы)
