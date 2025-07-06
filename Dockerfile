FROM python:3.7-slim
ENV PROJECT_HOME=/home/ibdn/practica_creativa
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./resources/web .
EXPOSE 5001
CMD ["python", "predict_flask.py"]

