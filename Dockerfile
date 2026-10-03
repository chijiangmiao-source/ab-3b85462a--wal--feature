FROM python:3.11-slim

WORKDIR /srv

# The service uses the Python standard library only; no pip install needed.
COPY app/ ./app/
COPY tests/ ./tests/

RUN chmod +x ./tests/verify.sh

EXPOSE 8080

HEALTHCHECK --interval=3s --timeout=3s --start-period=2s --retries=10 \
    CMD python3 -c "import json,urllib.request,sys; \
r=urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=2); \
sys.exit(0 if r.status==200 and json.load(r)['status']=='ok' else 1)"

CMD ["python3", "app/server.py"]
