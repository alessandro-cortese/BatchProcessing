FROM python:3.12.10

RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

RUN useradd -m spark -s /bin/bash

WORKDIR /home/spark

USER spark

COPY requirements.txt /home/spark/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt


CMD ["/bin/bash"]