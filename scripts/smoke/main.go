// Command smoke is a manual end-to-end check for a running Phase 1 gateway
// (DEV_MODE=true). It mints two identities, creates a conversation, connects
// both over WebSocket, sends a message from A, shows B receiving it, and shows
// an idempotent retry returning the same seq.
//
//	go run ./scripts/smoke            # against http://localhost:8080
//	BASE_URL=http://host:8080 go run ./scripts/smoke
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

func main() {
	base := os.Getenv("BASE_URL")
	if base == "" {
		base = "http://localhost:8080"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tokenA, userA := devToken(ctx, base)
	tokenB, userB := devToken(ctx, base)
	fmt.Printf("user A = %s\nuser B = %s\n", userA, userB)

	convID := createConversation(ctx, base, tokenA, userB)
	fmt.Printf("conversation = %s\n\n", convID)

	wsB := dial(ctx, base, tokenB)
	defer wsB.Close(websocket.StatusNormalClosure, "")
	readFrame(ctx, wsB, "B") // hello

	wsA := dial(ctx, base, tokenA)
	defer wsA.Close(websocket.StatusNormalClosure, "")
	readFrame(ctx, wsA, "A") // hello

	clientMsgID := uuid.NewString()
	send := map[string]any{
		"type":              "message.send",
		"request_id":        "req-1",
		"conversation_id":   convID,
		"client_message_id": clientMsgID,
		"kind":              "text",
		"body":              "hello from A",
	}
	writeFrame(ctx, wsA, send)
	fmt.Println("A -> message.send")
	readFrame(ctx, wsA, "A") // accepted (sync)
	readFrame(ctx, wsA, "A") // message.created (async, via chat.events)
	readFrame(ctx, wsB, "B") // message.created

	fmt.Println("\nA -> message.send (identical retry)")
	writeFrame(ctx, wsA, send)
	readFrame(ctx, wsA, "A") // accepted
	readFrame(ctx, wsA, "A") // message.created, duplicate=true, same seq (only to sender)

	fmt.Println("\nA -> message.edit seq=1")
	writeFrame(ctx, wsA, map[string]any{
		"type": "message.edit", "request_id": "req-2",
		"conversation_id": convID, "seq": 1, "body": "hello from A (edited)",
	})
	readFrame(ctx, wsA, "A") // accepted
	readFrame(ctx, wsA, "A") // message.updated
	readFrame(ctx, wsB, "B") // message.updated

	fmt.Println("\nA -> message.delete seq=1")
	writeFrame(ctx, wsA, map[string]any{
		"type": "message.delete", "request_id": "req-3",
		"conversation_id": convID, "seq": 1,
	})
	readFrame(ctx, wsA, "A") // accepted
	readFrame(ctx, wsA, "A") // message.deleted
	readFrame(ctx, wsB, "B") // message.deleted

	fmt.Println("\nB -> GET messages?after_seq=0")
	body := apiGet(ctx, base, tokenB, "/v1/conversations/"+convID+"/messages?after_seq=0")
	fmt.Println(string(body))
}

func devToken(ctx context.Context, base string) (token, userID string) {
	body := apiPost(ctx, base, "", "/v1/dev/token", map[string]any{})
	var out struct {
		Token  string `json:"token"`
		UserID string `json:"user_id"`
	}
	must(json.Unmarshal(body, &out))
	return out.Token, out.UserID
}

func createConversation(ctx context.Context, base, token, otherUser string) string {
	body := apiPost(ctx, base, token, "/v1/conversations", map[string]any{
		"type":       "direct",
		"member_ids": []string{otherUser},
	})
	var out struct {
		ID string `json:"id"`
	}
	must(json.Unmarshal(body, &out))
	return out.ID
}

func dial(ctx context.Context, base, token string) *websocket.Conn {
	url := wsScheme(base) + "/ws?access_token=" + token
	c, _, err := websocket.Dial(ctx, url, nil)
	must(err)
	return c
}

func writeFrame(ctx context.Context, c *websocket.Conn, v any) {
	b, err := json.Marshal(v)
	must(err)
	must(c.Write(ctx, websocket.MessageText, b))
}

func readFrame(ctx context.Context, c *websocket.Conn, who string) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := c.Read(rctx)
	must(err)
	var pretty bytes.Buffer
	_ = json.Indent(&pretty, data, "", "  ")
	fmt.Printf("  %s <- %s\n", who, pretty.String())
}

func apiPost(ctx context.Context, base, token, path string, payload any) []byte {
	b, err := json.Marshal(payload)
	must(err)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return do(req)
}

func apiGet(ctx context.Context, base, token, path string) []byte {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	return do(req)
}

func do(req *http.Request) []byte {
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		log.Fatalf("%s %s -> %d: %s", req.Method, req.URL.Path, resp.StatusCode, body)
	}
	return body
}

func wsScheme(base string) string {
	if len(base) > 5 && base[:5] == "https" {
		return "wss" + base[5:]
	}
	return "ws" + base[4:]
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
