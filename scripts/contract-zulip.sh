#!/usr/bin/env bash
# Boots one Zulip server from docker-zulip and makes a realm, a bot and a
# second user for the contract suite (test/contract/). Prints the suite's
# environment as KEY=value lines on stdout.
#
# Usage: scripts/contract-zulip.sh DOCKER_ZULIP_DIR
#   DOCKER_ZULIP_DIR: a checkout of zulip/docker-zulip at the commit CI pins.
set -euo pipefail

# ghcr.io/zulip/zulip-server:12.4-0, pinned by digest so a re-pushed tag
# can't change the server a green run was against.
image="ghcr.io/zulip/zulip-server@sha256:0a2b5760daaa69fc6fd425fed1cbd969be79c5842cd94443ce798243e7fb07af"

cd "${1:?usage: scripts/contract-zulip.sh DOCKER_ZULIP_DIR}"
export GITHUB_CI_IMAGE="$image"
export COMPOSE_FILE=compose.yaml:ci/base.yaml:ci/basic/compose.yaml
export COMPOSE_ENV_FILES=ci/base.env

docker compose pull -q >&2
docker compose up -d --no-build >&2
deadline=$((SECONDS + 600))
until docker inspect --format '{{.State.Health.Status}}' "$(docker compose ps -q zulip)" | grep -q '^healthy$'; do
  if ((SECONDS > deadline)); then
    echo "Zulip didn't turn healthy in 600 s" >&2
    docker compose logs zulip | tail -60 >&2
    exit 1
  fi
  sleep 2
done

realm="https://localhost"
human="human@example.com"
password="contract-$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"
docker compose exec -T -u zulip zulip /home/zulip/deployments/current/manage.py \
  create_realm 'Contract Realm' "$human" 'Contract Human' --password "$password" >&2

# The image's certificate is self-signed.
api=(curl -sSf --insecure)
human_key=$("${api[@]}" -X POST "$realm/api/v1/fetch_api_key" \
  --data-urlencode "username=$human" --data-urlencode "password=$password" | jq -r .api_key)
bot=$("${api[@]}" -X POST "$realm/api/v1/bots" -u "$human:$human_key" \
  --data-urlencode 'full_name=Contract Bot' --data-urlencode 'short_name=contract')
bot_id=$(jq -r .user_id <<<"$bot")
bot_key=$(jq -r .api_key <<<"$bot")
bot_email=$("${api[@]}" "$realm/api/v1/users/$bot_id" -u "$human:$human_key" | jq -r .user.email)

echo "ZULIP_REALM=$realm"
echo "ZULIP_EMAIL=$bot_email"
echo "ZULIP_API_KEY=$bot_key"
echo "ZULIP_CONTRACT_USER_EMAIL=$human"
echo "ZULIP_CONTRACT_USER_API_KEY=$human_key"
echo "NODE_TLS_REJECT_UNAUTHORIZED=0"
