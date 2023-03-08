FROM python:3.9-slim-buster AS builder
RUN apt-get update && apt-get install -y \
      gcc \
      libpq-dev \
      build-essential

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.9-slim-buster AS release

COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

WORKDIR /app
COPY thingsboard_gateway ./thingsboard_gateway
ENTRYPOINT ["python", "-m", "thingsboard_gateway.tb_gateway"]
