# The shared auth keyfile is baked into the image (not bind-mounted) because
# MongoDB 7.0 rejects a single-file bind-mount keyfile with "bad file". Owned by
# the mongodb user (999) with 0600 so mongod can read it. /data/db is a named
# volume at runtime, so nothing is stored here.
FROM mongo:7.0

USER root
COPY mongodb-keyfile /etc/mongodb-internal-keyfile
RUN chown mongodb:mongodb /etc/mongodb-internal-keyfile && \
    chmod 600 /etc/mongodb-internal-keyfile
USER mongodb