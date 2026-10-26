# The lakehouse's MCP server (server.py): query_metric over Cube's REST API, define_term over the glossary.
# Small on purpose: no lake drivers, no credentials but MCP_TOKEN and CUBEJS_API_SECRET.
ARG PYTHON_TAG=3.13.15-slim
FROM python:${PYTHON_TAG}
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --root-user-action=ignore -r /tmp/requirements.txt && rm /tmp/requirements.txt
RUN useradd --uid 1000 --create-home mcp
USER mcp
WORKDIR /app
EXPOSE 8000
CMD ["python", "/app/server.py"]
