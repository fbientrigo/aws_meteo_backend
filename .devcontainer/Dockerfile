# Usamos una imagen base ligera de Python 3.12
FROM python:3.12-slim

# Evitar archivos .pyc y buffers en stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependencias del sistema necesarias para HDF5/NetCDF
# gcc y pkg-config a veces se requieren para compilar bindings si no hay wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    pkg-config \
    libhdf5-dev \
    libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear un usuario no-root por seguridad
RUN useradd -m -u 1000 appuser

# Establecer directorio de trabajo
WORKDIR /app
ENV PYTHONPATH=/app

# Copiar requirements primero para aprovechar cache de Docker
COPY .devcontainer/requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY --chown=appuser:appuser . .

# Cambiar al usuario no-root
USER appuser

# Exponer el puerto
EXPOSE 8000

# Comando por defecto (ajustable en devcontainer/docker-compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
