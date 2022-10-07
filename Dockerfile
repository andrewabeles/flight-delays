FROM python:3.10.7

ADD requirements.txt .
ADD app.py .
ADD src/ ./src/
ADD data/processed/ ./data/processed/ 

RUN pip install -r requirements.txt

EXPOSE 8050

CMD ["python3", "./app.py"]