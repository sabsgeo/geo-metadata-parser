FROM python:3.10.11-slim-buster
COPY geo /
COPY main.py /
COPY requirements.txt /
RUN pip install -r /requirements.txt