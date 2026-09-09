# Proyecto Sello — Monitoreo y anticipación de riesgos climáticos en Juliaca

Proyecto del curso **Big Data** (UPeU, 2026-2) — equipo **BIG IMPACT**.

Pipeline de Big Data que combina histórico climático (16 años, Open-Meteo) con modelos predictivos para anticipar dos riesgos climáticos clave en Juliaca (altiplano andino, >3,800 msnm): **heladas** y **lluvia intensa**, que afectan directamente la producción agrícola local (papa, quinua, cebada).

📄 **Documentación completa:** [nilver987.github.io/big_data](https://nilver987.github.io/big_data/)

## Equipo

| Integrante | Dimensión |
|---|---|
| Salcca Aquino, Nilver | ❄️ Heladas |
| Arce Mayta, Efrain Nayder | 🌧️ Precipitación |

## Estructura del repositorio

```
docs/                       # Documentación del proyecto (MkDocs)
  dominio.md                 # Problema, arquitectura Lambda, fuentes de datos
  dimensiones/                # Preguntas y diseño de cada dimensión (heladas, precipitación)
  resultados/                 # Notebooks U1 exportados con gráficos y resultados
bigdata-u1/
  01_heladas_nilver.ipynb     # Notebook U1 — dimensión Heladas
  02_precipitacion_nayder.ipynb  # Notebook U1 — dimensión Precipitación
  data/
    clima_juliaca_horario.csv  # Dataset compartido: Open-Meteo, 2010-2026, horario
mkdocs.yml                   # Configuración del sitio de documentación
```

## Arquitectura

**Lambda**: capa batch (U1, construida) con PySpark + Parquet particionado + modelos de clasificación (`LogisticRegression`); capa speed/streaming (U2, en desarrollo) con Kafka + Spark Structured Streaming; capa de servicio con Grafana. Ver el diagrama completo en [Dominio del proyecto](https://nilver987.github.io/big_data/dominio/).

## Resultados U1 (resumen)

| Dimensión | Config ganadora | AUC | F1 |
|---|---|---|---|
| ❄️ Heladas | Estacionalidad + humedad/nubosidad/presión | 0.961 | 0.906 |
| 🌧️ Precipitación | Estacionalidad + humedad/nubosidad/presión | 0.943 | 0.874 |

Detalle completo, gráficos y código en [Resultados técnicos](https://nilver987.github.io/big_data/resultados/heladas/01_heladas_nilver/).

## Cómo correr los notebooks

Requiere un entorno con PySpark + Jupyter (ver el repo del curso [`262bigdata/lambda26`](https://github.com/262bigdata/lambda26) para el `Dockerfile`/`compose.yml` de referencia). Los notebooks leen `data/clima_juliaca_horario.csv` y corren de punta a punta sin intervención manual.
