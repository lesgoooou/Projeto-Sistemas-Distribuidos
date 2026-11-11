FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY painel.py index.html ./
EXPOSE 8080
CMD ["python", "painel.py"]
