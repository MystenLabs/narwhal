
# How to run more than the default 4 nodes with docker compose.

The simple steps are:

```
./gen.sh 6

# That will create a docker-compose.yaml file in ./validators-6/docker-compose.yaml

cd validators-6

docker compose up -d
docker compose logs -f

```


The grafana instance is exposed at http://localhost:3000/

Default user/pass is admin/admin.  You can 'skip' changing that since it's always
regenerated.
