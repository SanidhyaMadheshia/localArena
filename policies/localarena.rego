# LocalArena authorization policy (adr/0014). Query: POST /v1/data/localarena/authz
# input: {"action": "...", "subject": {"player_id": n, "roles": [...], "banned": bool},
#         "resource": {"player_id": n}}
# Decision: {"allow": bool, "reason": "..."}. Default deny.
package localarena.authz

import rego.v1

default allow := false

# Banned accounts can do nothing, whatever their roles.
banned if input.subject.banned

is_admin if "admin" in input.subject.roles

own_resource if input.resource.player_id == input.subject.player_id

# Players act on their own account only.
allow if {
	not banned
	"player" in input.subject.roles
	input.action in {"purchase", "lobby.create", "lobby.join", "lobby.leave", "presence.write", "game.ticket"}
	own_resource
}

# Admins may ban anyone except themselves (no self-lockout).
allow if {
	not banned
	is_admin
	input.action == "player.ban"
	not own_resource
}

reason := "banned" if banned
else := "allowed" if allow
else := "forbidden"

decision := {"allow": allow, "reason": reason}
