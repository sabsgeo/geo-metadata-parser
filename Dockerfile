FROM python:3.10.11-slim-buster
RUN mkdir /workspace
WORKDIR /workspace

COPY requirements.txt /workspace/
RUN pip install --upgrade pip
RUN pip install -r /workspace/requirements.txt

COPY geo /workspace/geo/
COPY helpers /workspace/helpers/
COPY main.py /workspace/
COPY main.sh /workspace/
RUN chmod +x /workspace/main.sh

ENTRYPOINT [ "/bin/bash", "main.sh" ]