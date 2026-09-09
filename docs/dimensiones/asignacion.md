# 03 · Dimensiones de análisis y fuentes previstas

## Tabla de asignación

*2 integrantes, 2 dimensiones cada uno.*

| Integrante | Tipo | Dimensión (como pregunta) |
|---|---|---|
| **NILVER** | Predictiva (U1, batch) | ¿En qué meses hay más de 30% de probabilidad de un día con helada (mínima < 0°C) en Juliaca, y qué tan bien se predice con estacionalidad + contexto meteorológico? |
| **NILVER** | Predictiva (U2, streaming) | ¿Cuándo se espera que la temperatura cruce el umbral de helada (0°C) en las próximas horas, según la tendencia actual del pronóstico? |
| **NAYDER** | Predictiva (U1, batch) | ¿En qué meses hay más de 30% de probabilidad de un día de lluvia significativa (acumulado > 2mm) en Juliaca, y qué tan bien se predice con estacionalidad + contexto meteorológico? |
| **NAYDER** | Predictiva (U2, streaming) | ¿Cuánta lluvia se espera en las próximas horas en Juliaca, y en qué momento se proyecta que supere el umbral de riesgo? |

## Detalle por dimensión

- **[❄️ Heladas](heladas.md)** — Nilver · U1 batch + U2 streaming
- **[🌧️ Precipitación](precipitacion.md)** — Nayder · U1 batch + U2 streaming
