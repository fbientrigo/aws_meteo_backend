Con el objetivo de crear una iamgen docker para un AWS Lambda sin Cold Start, actualmente por encima de 15 segundos
necesitamos crear el docker image

"""
docker build -f .devcontainer/Dockerfile .
"""

Luego corremos los tests
"""
docker run --rm aws_meteo_backend python -m pytest tests/
"""

____


El error que estás viendo es muy común: **construiste la imagen pero no le pusiste un nombre (tag)**, por lo que Docker no sabe cómo encontrarla cuando intentas ejecutar `docker run`.

En el log de construcción se ve que Docker la nombró automáticamente como `moby-dangling`, lo cual es básicamente una imagen "huérfana".

Aquí tienes los pasos exactos para corregirlo y realizar el **Smoke Test** con éxito:

### 1. Construir la imagen con un nombre (Tag)

Ejecuta este comando para asignarle el nombre `aws_meteo_backend`:

```powershell
docker build -t aws_meteo_backend -f .devcontainer/Dockerfile .

```

*(El parámetro `-t` es la clave, le indica a Docker el nombre que debe registrar en tu sistema local).*

### 2. Ejecutar los Tests

Ahora que la imagen tiene nombre, el comando de `run` debería funcionar:

```powershell
docker run --rm aws_meteo_backend python -m pytest tests/

```

---

### ⚠️ Advertencia Técnica (Posibles fallos en el Smoke Test)

Es muy probable que al correr los tests obtengas errores. Aquí te explico por qué y cómo interpretarlos según tu arquitectura actual:

1. **Falta de Credenciales de AWS:** Si tus tests (`tests/test_real_s3_integration.py` o `test_endpoints_sti.py`) intentan conectarse a S3 real, fallarán porque el contenedor no tiene tus credenciales de AWS.
* **Solución rápida para debug:** Pasa tus credenciales locales al contenedor (si usas el perfil `default` de AWS CLI):
```powershell
docker run --rm -v ${HOME}/.aws:/root/.aws:ro aws_meteo_backend python -m pytest tests/

```




2. **Rutas de Importación:** Como movimos todo a `/app` en el contenedor, si algún test hace `from main import app`, podría fallar si el `PYTHONPATH` no está configurado.
* **Solución:** El `Dockerfile` debería tener definido `ENV PYTHONPATH=/app`.


3. **Dependencias Eliminadas:** Si algún test utiliza `matplotlib` o `scikit-learn` (que acabamos de podar), el test fallará con un `ModuleNotFoundError`.
* **Acción:** Es el momento de aplicar el **Prompt 3** que te pasé: limpiar los tests para que solo validen el **Core Mínimo**.



### ¿Qué hacer si fallan los tests?

Si los tests fallan, por favor **copia y pega el error de pytest aquí**. Analizaremos si es un problema de conectividad a S3 o si todavía queda "grasa" (imports residuales) en el código que debemos limpiar para que el backend sea verdaderamente *lite*.