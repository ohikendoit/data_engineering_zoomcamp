FROM python: 3.9.1

import sys

RUN pip install pandas

print(sys.argv)

day = sys.argv[1]

WORKDIR /app
COPY pipeline.py

ENTRYPOINT["python", "pipeline.py"]