package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/authz"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/webhook"
)

// /v2 is the authenticated API (adr/0013, adr/0015). The acting player always
// comes from the verified token, never from the request body, which removes
// the v1 "act as any player_id" hole. Authorization decisions come from OPA.
//
// CSRF: v2 authenticates with an Authorization header, which browsers never
// attach automatically, so cross-site requests carry no credentials; there are
// no auth cookies. CORS only allows the known web origins.
func (s *Server) registerV2(mux *http.ServeMux) {
	authed := func(h http.HandlerFunc) http.Handler { return s.opts.Auth.Middleware(h) }
	mux.Handle("GET /v2/me", authed(s.me))
	mux.Handle("PUT /v2/me/profile", authed(s.updateProfile))
	mux.Handle("POST /v2/me/avatar", authed(s.importAvatar))
	mux.Handle("POST /v2/purchases", authed(s.purchaseV2))
	mux.Handle("POST /v2/game-tickets", authed(s.gameTicket))
	mux.Handle("POST /v2/admin/bans", authed(s.banPlayer))
	mux.HandleFunc("GET /v2/players/{id}", s.getProfile) // public view
	mux.HandleFunc("GET /v2/players/{id}/avatar", s.serveAvatar)
	mux.HandleFunc("GET /v2/cosmetics", s.listCosmetics)
	mux.HandleFunc("POST /v2/webhooks/match-results", s.matchResultWebhook) // HMAC-authenticated, not bearer
	mux.HandleFunc("OPTIONS /v2/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
}

// AllowedOrigins may call /v2 from a browser (CORS).
var AllowedOrigins = map[string]bool{"http://localhost:8080": true, "https://localhost:8443": true}

// securityHeaders applies to every response. The API only returns JSON or
// images, so the CSP forbids everything; nosniff stops a browser from
// interpreting a JSON or avatar body as HTML (stored-XSS defence in depth).
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Resource-Policy", "same-site") // ZAP 90004 (Spectre isolation)
		h.Set("Cache-Control", "no-store")
		if o := r.Header.Get("Origin"); o != "" && AllowedOrigins[o] && strings.HasPrefix(r.URL.Path, "/v2/") {
			h.Set("Access-Control-Allow-Origin", o)
			h.Set("Vary", "Origin")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key")
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
			h.Set("Access-Control-Max-Age", "600")
		}
		next.ServeHTTP(w, r)
	})
}

// caller maps the token to a player (creating it on first login) and loads
// ban state. Every v2 handler starts here.
func (s *Server) caller(r *http.Request) (authz.Subject, auth.Principal, error) {
	p, _ := auth.FromContext(r.Context())
	id, err := s.st.PlayerForSubject(r.Context(), p.Subject, p.Username, startingCredits)
	if err != nil {
		return authz.Subject{}, p, err
	}
	banned, _, err := s.st.Banned(r.Context(), id)
	return authz.Subject{PlayerID: id, Roles: p.Roles, Banned: banned}, p, err
}

func (s *Server) check(r *http.Request, action string, sub authz.Subject, target int64) error {
	if s.opts.Authz == nil {
		return authz.ErrUnavailable // fail closed: v2 never runs without a policy engine
	}
	return s.opts.Authz.Check(r.Context(), action, sub, authz.Resource{PlayerID: target})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	sub, _, err := s.caller(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	p, err := s.st.Profile(r.Context(), s.st.Primary, sub.PlayerID)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profile": p, "roles": sub.Roles, "banned": sub.Banned})
}

func (s *Server) purchaseV2(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header (1-64 chars) is required"})
		return
	}
	var req struct {
		CosmeticID int64 `json:"cosmetic_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	sub, _, err := s.caller(r)
	if err == nil {
		err = s.check(r, "purchase", sub, sub.PlayerID)
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !s.allow(w, r, "purchase:player:"+strconv.FormatInt(sub.PlayerID, 10), s.opts.PurchaseRL) {
		return
	}
	rc, err := s.st.Purchase(r.Context(), sub.PlayerID, req.CosmeticID, key, store.Conditional)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidate(r, profileKey(sub.PlayerID))
	s.stampLSN(w, r)
	writeJSON(w, map[bool]int{true: http.StatusOK, false: http.StatusCreated}[rc.Replayed], rc)
}

// gameTicket lets an authenticated, unbanned player join the gameserver
// (which verifies only the ticket's MAC; it has no database).
func (s *Server) gameTicket(w http.ResponseWriter, r *http.Request) {
	sub, _, err := s.caller(r)
	if err == nil {
		err = s.check(r, "game.ticket", sub, sub.PlayerID)
	}
	if err == nil && s.opts.Tickets == nil {
		err = authz.ErrUnavailable
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ticket": s.opts.Tickets.Issue(sub.PlayerID, time.Now()),
		"player_id": sub.PlayerID, "expires_in": int(auth.TicketTTL.Seconds())})
}

func (s *Server) banPlayer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PlayerID int64  `json:"player_id"`
		Reason   string `json:"reason"`
		Hours    int    `json:"hours"` // 0 = permanent
	}
	if !decode(w, r, &req) {
		return
	}
	if req.PlayerID <= 0 || req.Reason == "" || len(req.Reason) > 200 || req.Hours < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "player_id, reason (1-200 chars), hours >= 0 required"})
		return
	}
	sub, p, err := s.caller(r)
	if err == nil {
		err = s.check(r, "player.ban", sub, req.PlayerID)
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var until *time.Time
	if req.Hours > 0 {
		t := time.Now().Add(time.Duration(req.Hours) * time.Hour)
		until = &t
	}
	if err := s.st.Ban(r.Context(), req.PlayerID, req.Reason, p.Subject, until); err != nil {
		s.fail(w, r, err)
		return
	}
	s.log.InfoContext(r.Context(), "player banned", "player", req.PlayerID, "by", p.Subject, "hours", req.Hours) // audit trail
	w.WriteHeader(http.StatusNoContent)
}

// ValidDisplayName accepts 1-32 printable characters. It does not try to
// "sanitize HTML": the name is data, stored as typed and escaped on output
// (JSON encoding here, textContent in the client). Rejecting control and
// bidi-override characters prevents spoofing in other players' UIs.
func ValidDisplayName(n string) bool {
	if !utf8.ValidString(n) || strings.TrimSpace(n) == "" || utf8.RuneCountInString(n) > 32 {
		return false
	}
	for _, r := range n {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) {
			return false
		}
	}
	return true
}

func (s *Server) updateProfile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DisplayName string `json:"display_name"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !ValidDisplayName(req.DisplayName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "display_name: 1-32 printable characters"})
		return
	}
	sub, _, err := s.caller(r)
	if err == nil && sub.Banned {
		err = store.ErrBanned
	}
	if err == nil {
		err = s.st.SetDisplayName(r.Context(), sub.PlayerID, req.DisplayName)
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidate(r, profileKey(sub.PlayerID))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) importAvatar(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if !decode(w, r, &req) {
		return
	}
	sub, _, err := s.caller(r)
	if err == nil && sub.Banned {
		err = store.ErrBanned
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	img, ctype, err := FetchAvatar(ctx, s.opts.AvatarFetch, req.URL)
	if err != nil {
		var ue *UnsafeURLError
		if errors.As(err, &ue) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": ue.Error()})
		} else {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "could not import avatar"})
		}
		return
	}
	if err := s.st.SetAvatar(r.Context(), sub.PlayerID, ctype, img); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveAvatar(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ctype, img, err := s.st.Avatar(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// Defence in depth: the DB CHECK already limits content_type, and bytes were
	// sniffed on import; re-check so a bad row can never be served as HTML.
	if http.DetectContentType(img) != ctype || (ctype != "image/png" && ctype != "image/jpeg" && ctype != "image/webp") {
		s.fail(w, r, store.ErrNotFound)
		return
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Disposition", "inline; filename=avatar")
	w.Header().Set("Cache-Control", "public, max-age=300")
	// nosniff + CSP default-src 'none' (securityHeaders) also stop script execution.
	_, _ = w.Write(img) // #nosec G705 -- validated image bytes, see above
}

// matchResultWebhook accepts signed match results from a trusted sender (a
// tournament server). Order: read the raw body (bounded), verify signature and
// timestamp, then claim the delivery id and record in one transaction.
func (s *Server) matchResultWebhook(w http.ResponseWriter, r *http.Request) {
	if len(s.opts.WebhookSecrets) == 0 {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(r.Body) // MaxBytesReader from withTimeout caps this at 4 KiB
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body too large"})
		return
	}
	delivery := r.Header.Get(webhook.HeaderDelivery)
	if err := webhook.Verify(r.Header.Get(webhook.HeaderSignature), delivery, body, time.Now(), s.opts.WebhookSecrets...); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}
	var req struct {
		StartedAt time.Time           `json:"started_at"`
		EndedAt   time.Time           `json:"ended_at"`
		Players   []store.MatchPlayer `json:"players"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if dec.Decode(&req) != nil || len(req.Players) == 0 || len(req.Players) > 20 || !req.EndedAt.After(req.StartedAt) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid match result"})
		return
	}
	id, replayed, err := s.st.RecordMatchOnce(r.Context(), delivery, "webhook", req.StartedAt, req.EndedAt, req.Players)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if replayed {
		writeJSON(w, http.StatusOK, map[string]any{"replayed": true})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"match_id": id})
}
