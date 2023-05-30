FROM python:3.10.11-slim-buster
COPY geo /
COPY main.py /
COPY requirements.txt /
COPY main.sh /
RUN chmod +x /main.sh
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
ENTRYPOINT [ "/main.sh" ]