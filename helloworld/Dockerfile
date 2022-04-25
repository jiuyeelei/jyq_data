FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt

COPY . .

CMD ["python3", "app/app.py"]\
