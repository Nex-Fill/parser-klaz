# Klaz Parser API v1.0

Base URL: `http://45.131.214.9:8080`

---

## Auth

### POST /api/auth/register
```json
{ "email": "user@mail.com", "password": "pass123", "name": "User Name" }
```
Response: `{ "token": "eyJ...", "user": {...} }`

### POST /api/auth/login
```json
{ "email": "user@mail.com", "password": "pass123" }
```
Response: `{ "token": "eyJ...", "user": {...} }`

### GET /api/me
Headers: `Authorization: Bearer <token>`

---

## Search (main endpoint for users)

### POST /api/ads/search

The main endpoint. Returns ads with live metrics (views dynamics, price changes).

Headers: `Authorization: Bearer <token>`

```json
{
  "q": "iphone 15",

  "category_ids": ["173", "161"],

  "price_min": 50,
  "price_max": 800,

  "views_min": 10,
  "views_max": 10000,

  "poster_type": "PRIVATE",

  "is_active": true,
  "is_deleted": false,

  "date_from": "2026-03-01",
  "date_to": "2026-03-28",

  "has_images": true,

  "task_id": "task_123",

  "views_delta_1h_min": 5,
  "views_delta_1h_max": 100,

  "views_delta_24h_min": 20,
  "views_delta_24h_max": 5000,

  "views_per_hour_min": 2.0,

  "price_dropped": true,

  "sort_by": "views_per_hour",
  "sort_order": "desc",
  "page": 1,
  "per_page": 50
}
```

All fields are optional. Without any filters — returns all active ads.

**Available sort_by values:**
- `price` — by price
- `views` — by total views
- `created_at` — by when we first found it
- `updated_at` — by last update
- `start_date` — by when ad was posted on Kleinanzeigen
- `first_seen` — by when we first saw it
- `views_delta_1h` — trending NOW (views growth last hour)
- `views_delta_24h` — trending today
- `views_delta_7d` — trending this week
- `views_per_hour` — average views/hour (popularity)
- `snapshot_count` — most tracked ads

**Response:**
```json
{
  "Status": true,
  "data": {
    "ads": [
      {
        "id": "3364980402",
        "title": "iPhone 15 Pro 128 GB",
        "description": "Verkaufe mein iPhone...",
        "price_eur": 649,
        "contact_name": "Steffi",
        "category_id": "173",
        "ad_status": "ACTIVE",
        "poster_type": "PRIVATE",
        "start_date": "2026-03-27T21:15:54.000+0100",
        "url": "https://www.kleinanzeigen.de/s-anzeige/3364980402",
        "views": 342,
        "is_active": true,
        "is_deleted": false,
        "first_seen_at": "2026-03-27T20:00:00Z",
        "created_at": "2026-03-27T20:00:00Z",
        "metrics": {
          "views_current": 342,
          "price_current": 649,
          "views_delta_1h": 28,
          "views_delta_24h": 187,
          "views_delta_7d": 342,
          "views_per_hour": 14.2,
          "price_previous": 749,
          "price_min_seen": 649,
          "price_max_seen": 749,
          "price_dropped": true,
          "price_change_pct": -13.4,
          "snapshot_count": 24,
          "first_seen_at": "2026-03-25T10:30:00Z",
          "last_snapshot_at": "2026-03-27T16:00:00Z"
        }
      }
    ],
    "total": 47,
    "page": 1,
    "per_page": 20,
    "pages": 3
  }
}
```

---

## Ad Detail

### GET /api/ads/{id}
Full ad info with images and live metrics.
Auto-rechecks if data is older than 10 min.
Add `?fresh=true` to force recheck.

```json
{
  "Status": true,
  "data": {
    "ad": {
      "id": "3364980402",
      "title": "iPhone 15 Pro 128 GB",
      "description": "Full description...",
      "price_eur": 649,
      "contact_name": "Steffi",
      "category_id": "173",
      "location_id": "4935",
      "ad_status": "ACTIVE",
      "shipping_option": "DHL_001",
      "user_id": "143838212",
      "user_since_date": "2024-08-29T08:28:10.000+0200",
      "poster_type": "PRIVATE",
      "start_date": "2026-03-27T21:15:54.000+0100",
      "url": "https://www.kleinanzeigen.de/s-anzeige/3364980402",
      "views": 342,
      "is_active": true,
      "is_deleted": false,
      "images": [
        {
          "position": 0,
          "cdn_url": "http://klaz-img.govno.de/abc123.jpg",
          "hash": "abc123...",
          "preview_key": "abc123_preview.jpg"
        }
      ]
    },
    "metrics": {
      "views_current": 342,
      "views_delta_1h": 28,
      "views_delta_24h": 187,
      "views_per_hour": 14.2,
      "price_previous": 749,
      "price_dropped": true,
      "snapshot_count": 24
    }
  }
}
```

### POST /api/ads/{id}/recheck
Force instant recheck of a single ad.

### GET /api/ads/{id}/history?limit=100
All tracked changes (price, views, title, status, description).

```json
{
  "data": [
    { "field": "views", "old_value": "310", "new_value": "342", "changed_at": "2026-03-27T16:00:00Z" },
    { "field": "price", "old_value": "749", "new_value": "649", "changed_at": "2026-03-26T12:00:00Z" }
  ]
}
```

### GET /api/ads/{id}/statistics
Full data for charts. Returns:
- Raw snapshots (last 7 days) — for detailed zoom-in
- Hourly aggregates (all time) — for trend graph
- Daily aggregates (all time) — for overview
- All metrics

```json
{
  "data": {
    "ad_id": "3364980402",
    "metrics": { "views_delta_1h": 28, "views_per_hour": 14.2, "..." : "..." },
    "chart": {
      "views": [
        { "t": "2026-03-27T10:00:00Z", "v": 200 },
        { "t": "2026-03-27T11:00:00Z", "v": 228 }
      ],
      "price": [
        { "t": "2026-03-26T12:00:00Z", "v": 749 },
        { "t": "2026-03-27T00:00:00Z", "v": 649 }
      ],
      "hourly_views": [ { "t": "...", "v": 220 } ],
      "hourly_delta": [ { "t": "...", "v": 28 } ],
      "hourly_price": [ { "t": "...", "v": 649 } ],
      "daily_views": [ { "t": "...", "v": 300 } ],
      "daily_delta": [ { "t": "...", "v": 150 } ]
    },
    "total_changes": 34
  }
}
```

---

## Categories

### GET /api/categories?parent_id=
Without parent_id — root categories. With parent_id — children of that category.

### GET /api/categories/tree
Full flat list of all 157 categories.

### GET /api/categories/search?q=handy
Search categories by name.

---

## Tasks (user parsing jobs)

### POST /api/tasks
```json
{
  "task_name": "My Parse Job",
  "category_urls": ["https://www.kleinanzeigen.de/s-cat/173"],
  "max_pages_per_category": 10,
  "max_ads_to_check": 1000,
  "filters": {
    "price_min": 50,
    "price_max": 500,
    "poster_type": "PRIVATE"
  },
  "monitor_hours": 24
}
```

### GET /api/tasks
List your tasks.

### GET /api/tasks/{id}
Task status + progress (from Redis cache, instant).

### GET /api/tasks/{id}/ads?offset=0&limit=50
Ads found by this task.

### POST /api/tasks/{id}/pause
### POST /api/tasks/{id}/resume
### POST /api/tasks/{id}/stop
### DELETE /api/tasks/{id}

---

## Saved Filters

### GET /api/filters
Your saved filter presets.

### POST /api/filters
```json
{
  "name": "Cheap iPhones",
  "filters": { "price_min": 50, "price_max": 300 },
  "category_ids": ["173"],
  "notify_on_new": true,
  "notify_on_price_drop": true
}
```

### DELETE /api/filters/{id}

---

## Dashboard

### GET /api/dashboard
```json
{
  "data": {
    "total_ads": 50000,
    "active_ads": 48000,
    "deleted_ads": 2000,
    "total_images": 150000,
    "ads_today": 5000,
    "avg_price": 347,
    "trending_ads": 1200,
    "price_drops_24h": 340,
    "total_snapshots": 500000,
    "proxy_count": 75,
    "running_tasks": 2,
    "top_categories": [
      { "category_id": "173", "name": "Handy & Telefon", "count": 15000 }
    ]
  }
}
```

---

## System

### GET /health
```json
{ "status": "ok", "ads": 50000, "proxies": 75 }
```

### GET /api/proxy/stats
Proxy pool statistics with per-proxy latency and success rate.

### POST /api/ads/recheck-all
Trigger priority recheck of all ads.

---

## WebSocket

### GET /api/ws?token=JWT_TOKEN

Connect, then subscribe:
```json
{ "type": "subscribe", "channels": ["task:*"] }
```

Receives real-time updates:
```json
{ "type": "task:updates", "payload": { "task_id": "...", "status": "running", "progress": {...} } }
{ "type": "ad:updates", "payload": { "id": "...", "views": 342 } }
```

---

## Admin Panel

### http://45.131.214.9:8080/admin/
Login: admin / nexfill2026

Pages: Dashboard, Settings, Proxies, Tasks, Logs
All parser settings changeable live without restart.
