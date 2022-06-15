#!/usr/bin/env python3
"""Create a docker-compose.yaml file to stdout for --num # of nodes
"""

import sys
import argparse

HEADER = """---
version: "3.9"

volumes:
    prometheus_data: {}
    grafana_data: {}

services:

  prometheus:
    image: prom/prometheus:v2.36.0
    container_name: prometheus
    volumes:
      - ./prometheus:/etc/prometheus
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=200h'
      - '--web.enable-lifecycle'
    restart: unless-stopped
    expose:
      - 9090
    labels:
      org.label-schema.group: "monitoring"

  grafana:
    image: grafana/grafana:8.5.4
    container_name: grafana
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
    environment:
      - GF_SECURITY_ADMIN_USER=${ADMIN_USER:-admin}
      - GF_SECURITY_ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin}
      - GF_USERS_ALLOW_SIGN_UP=false
    restart: unless-stopped
    expose:
      - 3000
    ports:
      - 3000:3000
    labels:
      org.label-schema.group: "monitoring"

  nodeexporter:
    image: prom/node-exporter:v1.3.1
    container_name: nodeexporter
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - '--path.procfs=/host/proc'
      - '--path.rootfs=/rootfs'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    restart: unless-stopped
    expose:
      - 9100
    labels:
      org.label-schema.group: "monitoring"

"""

FOOTER = """...
"""

def main():
    "mainly main."
    parser = argparse.ArgumentParser(description="docker compose config generator")

    parser.add_argument("-n", default=4, type=int, help="number of primary+worker instances")
    parser.add_argument("-t", default="node.template", help="node template file")
    args = parser.parse_args()

    templ = open(args.t).read()

    print(HEADER)

    for i in range(args.n):
        tmp = "{:02d}".format(i)
        print(templ.format(counter=tmp, num=args.n))

    print(FOOTER)

if __name__ == '__main__':
    sys.exit(main())
