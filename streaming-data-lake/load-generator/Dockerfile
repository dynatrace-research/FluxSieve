FROM quay.io/fedora/python-311:latest
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.org/simple -r requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt
COPY generator-twofields.py .
CMD ["python", "generator-twofields.py"]
