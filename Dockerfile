ARG VERSION=latest \
    PYTHON_VERSION=3.10 \
    DEBIAN_FRONTEND=noninteractive
FROM python:${PYTHON_VERSION}-alpine AS release
RUN apk update && apk add --no-cache gcc libc-dev \
    && pip install --no-cache-dir kafkactl${VERSION+==$VERSION} \
    && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/local/bin/kafkactl"]
CMD ["--help"]