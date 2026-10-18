package localarena.authz_test

import data.localarena.authz
import rego.v1

player := {"player_id": 1, "roles": ["player"], "banned": false}

test_player_buys_for_self if authz.allow with input as {"action": "purchase", "subject": player, "resource": {"player_id": 1}}

test_player_cannot_buy_for_others if not authz.allow with input as {"action": "purchase", "subject": player, "resource": {"player_id": 2}}

test_banned_player_denied if {
	d := authz.decision with input as {"action": "purchase", "subject": object.union(player, {"banned": true}), "resource": {"player_id": 1}}
	not d.allow
	d.reason == "banned"
}

test_player_cannot_ban if not authz.allow with input as {"action": "player.ban", "subject": player, "resource": {"player_id": 2}}

test_admin_bans_other if authz.allow with input as {"action": "player.ban", "subject": {"player_id": 9, "roles": ["player", "admin"], "banned": false}, "resource": {"player_id": 2}}

test_admin_cannot_self_ban if not authz.allow with input as {"action": "player.ban", "subject": {"player_id": 9, "roles": ["admin"], "banned": false}, "resource": {"player_id": 9}}

test_unknown_action_denied if not authz.allow with input as {"action": "drop.tables", "subject": player, "resource": {"player_id": 1}}
