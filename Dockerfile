# Linux build/test environment for the IT305 project (the code needs Linux: sendfile(), MSG_MORE, ...).
# Same toolchain the project was developed and tested with: Ubuntu 24.04, gcc 13, Python 3.12.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential make python3 python3-matplotlib \
 && rm -rf /var/lib/apt/lists/*
# iproute2 provides `tc` for network emulation in the rehearsal rig (tools/rig.sh shape ...). Separate layer so the
# toolchain layer above stays cached when you rebuild.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 \
 && rm -rf /var/lib/apt/lists/*
# The project folder is bind-mounted at /app (docker run -v "$PWD":/app ...) so edits on your Mac show up
# immediately and results land in your folder. The topics dataset is mounted read-only at /data.
WORKDIR /app
CMD ["bash"]
