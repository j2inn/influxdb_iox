#################################################
#          InfluxDB IOx extractor 
#################################################

FROM debian:bookworm-20231030-slim as extractor

# Arguments which can be overrided at build time to specify InfluxDB file name and version.
ARG INFLUXDB_VERSION="0.1.0-alpha"
ARG INFLUXDB_TAR="influxdb_iox-x86_64-unknown-linux-gnu.tar.gz"

# Download compressed executable.
ADD https://github.com/j2inn/influxdb_iox/releases/download/v${INFLUXDB_VERSION}/${INFLUXDB_TAR} .

RUN tar -xzf ${INFLUXDB_TAR} && rm ${INFLUXDB_TAR}

#################################################
#              InfluxDB IOx image
#################################################

# With a two-stage build the image size is reduced since this one doesn't cache the dowloaded TAR.
FROM debian:bookworm-20231030-slim

WORKDIR /home

COPY --from=extractor /influxdb_iox .

# Create a mount point where InfluxDB stores its data.
VOLUME [ "/root/.influxdb_iox" ]

# Run the container as an executable.
ENTRYPOINT ["./influxdb_iox"]
