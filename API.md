# Klaz Parser API v2.0

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

The main endpoint. Returns ads with live metrics (views dynamics, price changes, favorites).

Headers: `Authorization: Bearer <token>`

```json
{
  "q": "iphone 15",
  "exclude": ["hülle", "case", "cover"],

  "category_ids": ["173", "161"],
  "location_ids": ["4935", "1234"],
  "seller_id": "143838212",

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

**Text search (`q`):**
- Splits query into words, ALL words must match (AND logic)
- Searches in title AND description
- Without explicit `sort_by` — title matches are prioritized, then sorted by views

**Exclude keywords (`exclude`):**
- Filters out ads where title OR description contains any of the excluded words
- Example: `"q": "iphone", "exclude": ["hülle", "case"]` — finds iPhones but not cases/covers

**Available sort_by values:**
- `price` — by price
- `views` — by total views
- `favorites` — by total favorites
- `created_at` — by when we first found it
- `updated_at` — by last update
- `start_date` — by when ad was posted on Kleinanzeigen
- `first_seen` — by when we first saw it
- `views_delta_1h` — trending NOW (views growth last hour)
- `views_delta_24h` — trending today
- `views_delta_7d` — trending this week
- `views_per_hour` — average views growth/hour
- `snapshot_count` — most tracked ads

> When sorting by metrics (views_delta_*, views_per_hour, snapshot_count), only ads with at least 2 snapshots are shown to avoid misleading data from newly discovered ads.

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
        "location_id": "4935",
        "ad_status": "ACTIVE",
        "poster_type": "PRIVATE",
        "start_date": "2026-03-27T21:15:54.000+0100",
        "url": "https://www.kleinanzeigen.de/s-anzeige/3364980402",
        "views": 342,
        "favorites": 12,
        "is_active": true,
        "is_deleted": false,
        "first_seen_at": "2026-03-27T20:00:00Z",
        "created_at": "2026-03-27T20:00:00Z",
        "thumbnail": "http://klaz-img.govno.de/abc123_preview.jpg",
        "metrics": {
          "views_current": 342,
          "favorites_current": 12,
          "price_current": 649,
          "views_delta_1h": 28,
          "views_delta_24h": 187,
          "views_delta_7d": 342,
          "views_per_hour": 14.2,
          "favorites_delta_1h": 2,
          "favorites_delta_24h": 8,
          "favorites_delta_7d": 12,
          "favorites_per_hour": 0.5,
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
    "per_page": 50,
    "pages": 1
  }
}
```

---

## Export

### POST /api/ads/export

Same filters as search. Returns CSV file (up to 5000 rows).

Headers: `Authorization: Bearer <token>`

```json
{
  "category_ids": ["173"],
  "price_dropped": true
}
```

Response: `Content-Type: text/csv` with columns:
`id, title, price_eur, views, category_id, location_id, poster_type, ad_status, url, start_date, views_per_hour, views_delta_1h, views_delta_24h, price_dropped, price_change_pct, first_seen_at`

---

## Batch Compare

### POST /api/ads/batch

Get multiple ads with full details, images, and metrics in one request.

Headers: `Authorization: Bearer <token>`

```json
{
  "ids": ["3364980402", "3364980403", "3364980404"]
}
```
Max 50 IDs per request.

Response:
```json
{
  "Status": true,
  "data": {
    "ads": [
      {
        "id": "3364980402",
        "title": "iPhone 15 Pro",
        "price_eur": 649,
        "views": 342,
        "favorites": 12,
        "location_id": "4935",
        "user_id": "143838212",
        "user_since_date": "2024-08-29",
        "thumbnail": "http://klaz-img.govno.de/abc123_preview.jpg",
        "images": [
          { "position": 0, "cdn_url": "http://klaz-img.govno.de/abc123.jpg" }
        ],
        "metrics": { "..." : "..." }
      }
    ],
    "total": 3
  }
}
```

---

## Ad Detail

### GET /api/ads/{id}
Full ad info with images and live metrics.
Add `?fresh=true` to force instant recheck from Kleinanzeigen.

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
      "favorites": 12,
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
      "favorites_current": 12,
      "price_current": 649,
      "views_delta_1h": 28,
      "views_delta_24h": 187,
      "views_delta_7d": 342,
      "views_per_hour": 14.2,
      "favorites_delta_1h": 2,
      "favorites_delta_24h": 8,
      "favorites_delta_7d": 12,
      "favorites_per_hour": 0.5,
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
}
```

### POST /api/ads/{id}/recheck
Force instant recheck of a single ad.

### GET /api/ads/{id}/history?limit=100
Tracked changes: price, title, description, ad_status, poster_type.

> Views are NOT tracked in history (use snapshots/metrics instead). Only meaningful changes are recorded — whitespace-only description edits and price changes from/to 0 are filtered out.

```json
{
  "data": [
    { "field": "price", "old_value": "749", "new_value": "649", "changed_at": "2026-03-26T12:00:00Z" },
    { "field": "ad_status", "old_value": "ACTIVE", "new_value": "DELETED", "changed_at": "2026-03-27T10:00:00Z" }
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
      "favorites": [
        { "t": "2026-03-27T10:00:00Z", "v": 8 },
        { "t": "2026-03-27T11:00:00Z", "v": 10 }
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

## Sellers

### GET /api/sellers/{user_id}
Seller profile with stats.

```json
{
  "Status": true,
  "data": {
    "user_id": "143838212",
    "name": "Steffi",
    "since_date": "2024-08-29T08:28:10.000+0200",
    "total_ads": 15,
    "active_ads": 12
  }
}
```

### GET /api/sellers/{user_id}/ads?offset=0&limit=50
All ads from a specific seller with metrics and thumbnails.

---

## Categories

### GET /api/categories?parent_id=
Without parent_id — root categories. With parent_id — children of that category.

### GET /api/categories/tree
Full flat list of all categories.

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

## Saved Filters + Notifications

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
When `notify_on_new` or `notify_on_price_drop` is true, the system checks every 10 minutes for matching ads and creates notifications automatically.

### DELETE /api/filters/{id}

### GET /api/notifications?unread=true&limit=50
```json
{
  "Status": true,
  "data": {
    "notifications": [
      {
        "id": 42,
        "type": "new_ads",
        "title": "5 new ads matching \"Cheap iPhones\"",
        "body": "• iPhone 15 128GB — €299\n• iPhone 14 Pro — €350\n...",
        "data": { "filter_id": "abc123", "count": 5 },
        "is_read": false,
        "created_at": "2026-03-28T10:00:00Z"
      },
      {
        "id": 41,
        "type": "price_drop",
        "title": "3 price drops matching \"Cheap iPhones\"",
        "body": "• iPhone 15 Pro — €549\n...",
        "data": { "filter_id": "abc123", "count": 3 },
        "is_read": false,
        "created_at": "2026-03-28T09:50:00Z"
      }
    ],
    "unread_count": 2
  }
}
```

### POST /api/notifications/{id}/read
Mark single notification as read.

### POST /api/notifications/read-all
Mark all notifications as read.

---

## Dashboard

### GET /api/dashboard
```json
{
  "data": {
    "total_ads": 1127006,
    "active_ads": 1100000,
    "deleted_ads": 27006,
    "total_images": 3500000,
    "ads_today": 15000,
    "avg_price": 347,
    "trending_ads": 32000,
    "price_drops_24h": 968,
    "total_snapshots": 5000000,
    "proxy_count": 23,
    "running_tasks": 2,
    "top_categories": [
      { "category_id": "173", "name": "Handy & Telefon", "count": 150000 }
    ]
  }
}
```

---

## System

### GET /health
```json
{ "status": "ok", "ads": 1127006, "proxies": 23 }
```

### GET /api/proxy/stats
Proxy pool statistics. **Admin only** (requires admin JWT).

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
{ "type": "ad:updates", "payload": { "id": "...", "views": 342, "favorites": 12 } }
```

---

## Admin Panel

### http://45.131.214.9:8080/admin/
All parser settings changeable live without restart.
