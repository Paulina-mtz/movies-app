# Movies App

Aplicación web basada en el dataset MovieLens que permite consultar películas,
sus valoraciones y rankings.

## Seguridad
Las credenciales de la base de datos NO se almacenan en el repositorio.
Se proporciona una plantilla de Secret de Kubernetes:

k8s/01-postgres-secret.template.yaml

Para desplegar en Kubernetes es necesario:
1. Copiar la plantilla a `01-postgres-secret.yaml`
2. Definir una contraseña real
3. Aplicar el Secret al clúster con kubectl
