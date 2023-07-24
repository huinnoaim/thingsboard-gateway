FROM python:3.11-slim-buster AS builder
RUN apt-get update && apt-get install -y \
      gcc \
      libpq-dev \
      build-essential

WORKDIR /app
COPY modules/alarm/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.11-slim-buster AS release

COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

WORKDIR /app
COPY modules/connectors ./connectors/
COPY modules/alarm/*.py ./alarm/
COPY modules/alarm/config.yaml ./
ENTRYPOINT ["python", "-m", "alarm.main"]
