#!/usr/bin/env bash
set -euo pipefail

# Idempotent bootstrap of the rs0 replica set (auth-enabled). Assumes each
# mongod already runs with --replSet rs0 --auth --keyFile.

AUTH="mongodb://${MONGO_ROOT_USERNAME}:${MONGO_ROOT_PASSWORD}@%s:27017/admin?authSource=admin"

echo "Waiting for all mongod nodes to accept connections..."
for h in mongodb1 mongodb2 mongodb3; do
  until mongosh "$(printf "$AUTH" "$h")" --quiet --eval 'quit(db.runCommand("ping").ok ? 0 : 1)' >/dev/null 2>&1; do
    echo "${h} not ready / not authenticating, retrying..."
    sleep 3
  done
done

echo "All nodes reachable + authenticating."

PRIMARY="$(printf "$AUTH" "mongodb1")"

echo "Checking if replica set rs0 is already initialized..."
if mongosh "$PRIMARY" --quiet --eval '
  const s = rs.status();
  print("already initialized: members=" + s.members.length);
' >/dev/null 2>&1; then
  echo "Replica set already initialized."
else
  echo "Initializing replica set rs0..."
  mongosh "$PRIMARY" <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongodb1:27017" },
    { _id: 1, host: "mongodb2:27017" },
    { _id: 2, host: "mongodb3:27017" }
  ]
})
EOF
  echo "rs.initiate() issued."
fi

echo "Ensuring the admin user exists (idempotent)..."
mongosh "$PRIMARY" <<EOF
if (!db.getUser("${MONGO_ROOT_USERNAME}")) {
  db.createUser({
    user: "${MONGO_ROOT_USERNAME}",
    pwd: "${MONGO_ROOT_PASSWORD}",
    roles: [ { role: "root", db: "admin" } ]
  });
  print("admin user created");
} else {
  print("admin user already exists");
}
EOF

echo "Waiting for a primary..."
until mongosh "$PRIMARY" --quiet --eval '
  const s = rs.status();
  const p = s.members.find(m => m.stateStr === "PRIMARY");
  quit(p ? 0 : 1);
' >/dev/null 2>&1; do
  sleep 3
done
echo "Primary elected."

echo "Replica set init complete."
