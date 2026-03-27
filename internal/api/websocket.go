package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type WSMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type WSClient struct {
	conn   *websocket.Conn
	userID string
	send   chan []byte
	subs   map[string]bool
	mu     sync.RWMutex
}

type WSHub struct {
	clients map[string]map[*WSClient]bool
	mu      sync.RWMutex
	rdb     *redis.Client
	secret  []byte
}

func NewWSHub(rdb *redis.Client, jwtSecret []byte) *WSHub {
	return &WSHub{
		clients: make(map[string]map[*WSClient]bool),
		rdb:     rdb,
		secret:  jwtSecret,
	}
}

func (hub *WSHub) Run(ctx context.Context) {
	channels := []string{"ad:updates", "task:updates", "recheck:progress", "config:changed"}
	pubsub := hub.rdb.Subscribe(ctx, channels...)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			hub.handleRedisMessage(msg)
		}
	}
}

func (hub *WSHub) handleRedisMessage(msg *redis.Message) {
	wsMsg := WSMessage{Type: msg.Channel, Payload: json.RawMessage(msg.Payload)}
	data, _ := json.Marshal(wsMsg)

	hub.mu.RLock()
	defer hub.mu.RUnlock()

	switch msg.Channel {
	case "task:updates":
		var task struct {
			UserID string `json:"user_id"`
			ID     string `json:"task_id"`
		}
		json.Unmarshal([]byte(msg.Payload), &task)
		if clients, ok := hub.clients[task.UserID]; ok {
			for client := range clients {
				client.mu.RLock()
				subscribed := client.subs["task:"+task.ID] || client.subs["task:*"]
				client.mu.RUnlock()
				if subscribed {
					select {
					case client.send <- data:
					default:
					}
				}
			}
		}
	default:
		for _, clients := range hub.clients {
			for client := range clients {
				select {
				case client.send <- data:
				default:
				}
			}
		}
	}
}

func (hub *WSHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "token required", 401)
		return
	}

	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return hub.secret, nil
	})
	if err != nil || !token.Valid {
		http.Error(w, "invalid token", 401)
		return
	}
	claims := token.Claims.(jwt.MapClaims)
	userID, _ := claims["sub"].(string)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &WSClient{
		conn:   conn,
		userID: userID,
		send:   make(chan []byte, 256),
		subs:   map[string]bool{"task:*": true},
	}

	hub.mu.Lock()
	if hub.clients[userID] == nil {
		hub.clients[userID] = make(map[*WSClient]bool)
	}
	hub.clients[userID][client] = true
	hub.mu.Unlock()

	go client.writePump()
	go client.readPump(hub)
}

func (c *WSClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			c.conn.WriteMessage(websocket.TextMessage, msg)
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *WSClient) readPump(hub *WSHub) {
	defer func() {
		hub.mu.Lock()
		if clients, ok := hub.clients[c.userID]; ok {
			delete(clients, c)
			if len(clients) == 0 {
				delete(hub.clients, c.userID)
			}
		}
		hub.mu.Unlock()
		close(c.send)
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var msg struct {
			Type     string   `json:"type"`
			Channels []string `json:"channels"`
		}
		if json.Unmarshal(message, &msg) != nil {
			continue
		}

		if msg.Type == "subscribe" {
			c.mu.Lock()
			for _, ch := range msg.Channels {
				c.subs[ch] = true
			}
			c.mu.Unlock()
			log.Debug().Str("user", c.userID).Strs("channels", msg.Channels).Msg("ws subscribed")
		} else if msg.Type == "unsubscribe" {
			c.mu.Lock()
			for _, ch := range msg.Channels {
				delete(c.subs, ch)
			}
			c.mu.Unlock()
		}
	}
}
