FROM python:3.12-bookworm

COPY . .
RUN pip install -e .[dev,test]
