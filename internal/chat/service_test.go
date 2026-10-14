package chat

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestClampLimit(t *testing.T) {
	cases := []struct{ in, def, max, want int }{
		{0, 100, 200, 100},
		{-5, 100, 200, 100},
		{50, 100, 200, 50},
		{500, 100, 200, 200},
		{200, 100, 200, 200},
	}
	for _, c := range cases {
		if got := clampLimit(c.in, c.def, c.max); got != c.want {
			t.Errorf("clampLimit(%d,%d,%d) = %d, want %d", c.in, c.def, c.max, got, c.want)
		}
	}
}

func TestValidateSend(t *testing.T) {
	conv := uuid.NewString()
	user := uuid.NewString()
	cmid := uuid.NewString()

	t.Run("defaults kind to text", func(t *testing.T) {
		p := SendMessageParams{ConversationID: conv, SenderID: user, ClientMessageID: cmid, Body: "hi"}
		if err := validateSend(&p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Kind != string(MessageText) {
			t.Fatalf("kind = %q, want %q", p.Kind, MessageText)
		}
	})

	t.Run("rejects non-uuid conversation", func(t *testing.T) {
		p := SendMessageParams{ConversationID: "nope", SenderID: user, ClientMessageID: cmid, Body: "hi"}
		if err := validateSend(&p); !errors.Is(err, ErrValidation) {
			t.Fatalf("err = %v, want ErrValidation", err)
		}
	})

	t.Run("rejects empty text body", func(t *testing.T) {
		p := SendMessageParams{ConversationID: conv, SenderID: user, ClientMessageID: cmid, Kind: "text"}
		if err := validateSend(&p); !errors.Is(err, ErrValidation) {
			t.Fatalf("err = %v, want ErrValidation", err)
		}
	})

	t.Run("rejects unknown kind", func(t *testing.T) {
		p := SendMessageParams{ConversationID: conv, SenderID: user, ClientMessageID: cmid, Kind: "carrier-pigeon", Body: "x"}
		if err := validateSend(&p); !errors.Is(err, ErrValidation) {
			t.Fatalf("err = %v, want ErrValidation", err)
		}
	})

	t.Run("rejects oversize body", func(t *testing.T) {
		big := make([]byte, maxTextBodyLen+1)
		for i := range big {
			big[i] = 'a'
		}
		p := SendMessageParams{ConversationID: conv, SenderID: user, ClientMessageID: cmid, Body: string(big)}
		if err := validateSend(&p); !errors.Is(err, ErrValidation) {
			t.Fatalf("err = %v, want ErrValidation", err)
		}
	})
}
