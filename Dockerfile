FROM python:3.11-slim

WORKDIR /app

# 复制依赖并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY app.py .

# 创建持久化数据目录
VOLUME /app/data
ENV DB_PATH=/app/data/eztv_torrents.db
ENV PORT=8000

EXPOSE 8000

CMD ["python", "app.py"]