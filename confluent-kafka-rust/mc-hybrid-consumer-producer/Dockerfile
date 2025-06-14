FROM python:3.13-bookworm

WORKDIR /app

RUN apt-get update && \
    apt-get install -y build-essential curl librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install maturin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Cargo.toml .
COPY src ./src/

RUN maturin build --release --out dist

RUN pip install --no-cache-dir dist/*.whl

COPY main.py .

CMD ["python", "main.py"]