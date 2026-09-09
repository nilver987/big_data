# 02 · Dominio del proyecto

## Nombre del proyecto

**Monitoreo y anticipación de riesgos climáticos en Juliaca**

## Problema o necesidad que resuelve

Juliaca, en el altiplano andino (>3,800 msnm), está expuesta a heladas nocturnas y lluvias intensas de temporada que afectan directamente la producción agrícola local (papa, quinua, cebada). No existe una herramienta accesible que combine el histórico climático con proyecciones de corto plazo para que agricultores y gestores de riesgo local anticipen estos eventos y actúen a tiempo (proteger cultivos, planificar riego, emitir alertas).

## Dominio de datos

Meteorología aplicada a agricultura y gestión de riesgo — variables horarias y diarias de temperatura y precipitación.

!!! question "Pregunta central de negocio"
    ¿Cómo anticipar condiciones climáticas de riesgo (heladas y lluvias intensas) en Juliaca para apoyar decisiones agrícolas y de gestión de riesgo local?

## Usuarios / actores principales

Agricultores y asociaciones agrarias de la zona; gestores de riesgo local; y, como actor automatizado, el propio sistema de alertas que consume las salidas del pipeline.

## Arquitectura Big Data prevista

<p class="pill">LAMBDA</p>

El proyecto necesita dos vistas complementarias sobre el mismo origen de datos:

- **(a) Vista histórica batch** — describe el pasado y entrena un modelo predictivo que se calcula una vez por corrida (foto fija).
- **(b) Vista en tiempo casi real** — un modelo de series de tiempo se actualiza dato por dato conforme llega el pronóstico.

!!! note "¿Por qué no Kappa?"
    Kappa no se justifica porque el reprocesamiento de 16 años de histórico horario (~146,000 registros) es más eficiente como batch por lotes que como replay de un log de eventos único, y porque el curso pide explícitamente ambas capas (U1 batch + U2 streaming) tratadas de forma distinta.

## Fuentes de datos

=== "Batch"

    **Open-Meteo Historical Weather API** (`archive-api.open-meteo.com`) — datos abiertos, sin autenticación. Coordenadas de Juliaca (-15.50082, -70.13980), 2010-2026 (16 años, ~146,000 registros horarios), variables horarias `temperature_2m`, `precipitation`, `relative_humidity_2m`, `cloud_cover`, `wind_speed_10m` y `surface_pressure`.

=== "Streaming"

    **Open-Meteo Forecast API** (`api.open-meteo.com`), consultada por un script productor que hace *polling* cada hora sobre las mismas coordenadas y publica cada lectura como evento en un tópico Kafka (`clima-juliaca`) — mismo origen físico que el batch, simulando la llegada de un sensor en vivo, tal como el curso permite cuando no hay un sensor físico propio.

## ¿Continúa un proyecto anterior o es un dominio nuevo?

Es un proyecto nuevo del equipo: el enfoque es monitoreo climático, pero se rediseñó el dominio de datos y la arquitectura (de Kappa , a Lambda con fuentes abiertas Open-Meteo).
