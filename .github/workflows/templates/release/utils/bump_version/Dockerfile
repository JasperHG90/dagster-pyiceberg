FROM python:3.11-slim

COPY requirements.txt /conf/requirements.txt

RUN pip install --no-cache-dir -r /conf/requirements.txt

COPY bump_version.py /bump_version.py

ENTRYPOINT ["python", "/bump_version.py"]
