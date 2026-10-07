FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir dagster==1.12.21 dagster-postgres==0.28.21 'sqlalchemy<2.1'
COPY pyproject.toml /adapter/pyproject.toml
COPY dagster_miren /adapter/dagster_miren
RUN pip install --no-cache-dir /adapter
COPY jobs.py /app/jobs.py
ENV PYTHONPATH=/app
CMD ["dagster", "api", "grpc", "-m", "jobs", "-h", "0.0.0.0", "-p", "4000"]
