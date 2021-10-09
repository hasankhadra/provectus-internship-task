FROM python:3.7.12-bullseye
WORKDIR /code
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV MINIO_ACCESS_KEY=minio-access-key
ENV MINIO_SECRET_KEY=minio-secret-key
ENV POSTGRES_DB=internship
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_HOST=db
ENV MINIO_HOST=minio
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
EXPOSE 5000
COPY . .
CMD ["python3", "app.py"]