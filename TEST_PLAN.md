# Test Plan

## Как запустить

```bash
# 1. Поднять инфраструктуру
docker-compose up -d postgres redis

# 2. Подождать пока postgres готов
docker-compose exec postgres pg_isready -U klaz

# 3. Запустить миграции (goose)
goose -dir migrations postgres "postgres://klaz:klaz@localhost:5432/klaz?sslmode=disable" up

# 4. Создать .env из .env.example
cp .env.example .env
# Заполнить S3_ACCESS_KEY, S3_SECRET_KEY если нужны картинки

# 5. Запустить сервер
go run ./cmd/server/

# Или через Docker
docker-compose up --build
```

## Тесты по endpoint'ам

### 1. Health check
```bash
curl http://localhost:8080/health
# Ожидание: {"status":"ok","ads":0,"proxies":N}
```

### 2. Регистрация + логин
```bash
# Register
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@test.com","password":"test123","name":"Test"}'
# Ожидание: {"token":"eyJ...","user":{...}}

# Login
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@test.com","password":"test123"}'
# Сохранить token для дальнейших запросов
TOKEN="eyJ..."
```

### 3. Категории (без авторизации)
```bash
# Список корневых
curl http://localhost:8080/api/categories

# Полное дерево
curl http://localhost:8080/api/categories/tree

# Поиск
curl "http://localhost:8080/api/categories/search?q=handy"

# Принудительная синхронизация
curl -X POST http://localhost:8080/api/categories/sync
```

### 4. Создание задачи парсинга
```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "Test Parse Handys",
    "category_urls": ["https://www.kleinanzeigen.de/s-cat/161"],
    "max_pages_per_category": 2,
    "max_ads_to_check": 20,
    "filters": {
      "price_min": 10,
      "price_max": 500,
      "poster_type": "PRIVATE"
    }
  }'
# Сохранить task_id

TASK_ID="task_..."
```

### 5. Отслеживание задачи
```bash
# Статус
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/tasks/$TASK_ID

# Список задач
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/tasks

# Объявления задачи
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/api/tasks/$TASK_ID/ads?limit=10"
```

### 6. Поиск объявлений с метриками
```bash
curl -X POST http://localhost:8080/api/ads/search \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "q": "iphone",
    "price_min": 100,
    "price_max": 800,
    "sort_by": "views",
    "sort_order": "desc",
    "page": 1,
    "per_page": 10
  }'
# Ожидание: ads[] с metrics{} внутри каждого
```

### 7. Детали объявления + instant recheck
```bash
AD_ID="..."

# Обычный запрос (auto recheck если stale >10мин)
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/ads/$AD_ID

# Принудительный recheck
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/ads/$AD_ID/recheck

# Ожидание: ad{} + metrics{} + images[]
```

### 8. Статистика / графики
```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/ads/$AD_ID/statistics
# Ожидание: metrics{}, chart{views[], price[], hourly_views[], daily_views[]}, total_changes

curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/ads/$AD_ID/history
# Ожидание: [{field, old_value, new_value, changed_at}...]
```

### 9. Saved filters
```bash
# Создать
curl -X POST http://localhost:8080/api/filters \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Cheap iPhones",
    "filters": {"price_min": 50, "price_max": 300},
    "category_ids": ["173"],
    "notify_on_new": true,
    "notify_on_price_drop": true
  }'

# Список
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/filters
```

### 10. Dashboard
```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/dashboard
# Ожидание: total_ads, active_ads, deleted_ads, trending_ads, price_drops_24h, top_categories...
```

### 11. Admin panel
```bash
# Открыть в браузере: http://localhost:8080/admin/
# Логин: admin / admin (или ADMIN_USER/ADMIN_PASSWORD из .env)

# JSON stats для live-polling
curl -u admin:admin http://localhost:8080/admin/api/stats
```

### 12. WebSocket
```javascript
// В браузере console:
const ws = new WebSocket('ws://localhost:8080/api/ws?token=' + TOKEN);
ws.onmessage = (e) => console.log(JSON.parse(e.data));
ws.onopen = () => {
  ws.send(JSON.stringify({type: 'subscribe', channels: ['task:*']}));
};
// Запустить задачу парсинга — в консоли появятся обновления прогресса
```

### 13. Proxy stats
```bash
curl http://localhost:8080/api/proxy/stats
# Ожидание: count, stats[] с URL/Alive/Latency/SuccessRate
```

### 14. Полный E2E flow
```bash
# 1. Register → 2. Sync categories → 3. Create task → 4. Wait for completion
# → 5. Search ads → 6. View ad detail → 7. Check statistics →
# → 8. Wait 1 hour → 9. Check history (views should change)
# → 10. Check dashboard (ads_today should increment)
```

## Что проверить вручную

- [ ] Прокси загружаются с https://sh.govno.de/proxies/text?country=Germany&maxTimeout=2000
- [ ] Категории синхронизируются (157 штук)
- [ ] Парсинг работает через прокси, объявления сохраняются в БД
- [ ] Картинки загружаются на Wasabi S3
- [ ] Snapshots записываются (проверить `SELECT COUNT(*) FROM ad_snapshots`)
- [ ] Metrics обновляются каждые 5 мин (`SELECT * FROM ad_metrics LIMIT 5`)
- [ ] Recheck помечает удалённые объявления (`is_deleted=true`)
- [ ] Admin panel: settings сохраняются и подхватываются без рестарта
- [ ] Admin panel: force recheck / force category sync работают
- [ ] WebSocket получает обновления при парсинге задачи
