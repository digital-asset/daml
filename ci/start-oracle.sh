set -euo pipefail
eval "$(./dev-env/bin/dade-assist)"
docker login --username "$DOCKER_LOGIN" --password "$DOCKER_PASSWORD"
IMAGE=digitalasset/oracle:enterprise-19.3.0-preloaded-20210325-22-be14fb7
docker pull $IMAGE
# Cleanup stray containers that might still be running from
# another build that didn’t get shut down cleanly.
docker rm -f oracle || true
# Oracle does not like if you connect to it via localhost if it’s running in the container.
# Interestingly it works if you use the external IP of the host so the issue is
# not the host it is listening on (it claims for that to be 0.0.0.0).
# --network host is a cheap escape hatch for this.
docker run -d --rm --name oracle --network host -e ORACLE_PWD=$ORACLE_PWD $IMAGE
testConnection() {
    docker exec oracle bash -c 'sqlplus -L '"$ORACLE_USERNAME"'/'"$ORACLE_PWD"'@//localhost:'"$ORACLE_PORT"'/ORCLPDB1 <<< "select * from dba_users;"; exit $?' >/dev/null
}
until testConnection
do
  echo "Could not connect to Oracle, trying again..."
  sleep 1
done
