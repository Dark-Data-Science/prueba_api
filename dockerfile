# Dockerfile
FROM python:3.7

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar el archivo de requisitos primero
COPY requirements.txt ./requirements.txt

# Instalar las dependencias de tu aplicación
RUN pip install -r requirements.txt

# Copiar los archivos de tu aplicación al contenedor
COPY . /app

# Iniciar la aplicación usando Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]