# Pull base image
FROM python:3.6-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y libpq-dev gcc
# need gcc to compile psycopg2
RUN pip3 install psycopg2~=2.6
RUN apt-get autoremove -y gcc
  
# Copy project
COPY . /code/
