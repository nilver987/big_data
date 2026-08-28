# Alcance del proyecto

<div class="scope-grid" markdown>
<div class="scope-card yes" markdown>
### ✓ Sí cubre

Tendencia histórica mensual con modelo predictivo batch (regresión lineal) y proyección de corto plazo con modelo de series de tiempo corriendo en vivo sobre Spark Structured Streaming, para dos riesgos climáticos clave de Juliaca —heladas y lluvia intensa—, integrados en un único tablero de Grafana con alertas.
</div>
<div class="scope-card no" markdown>
### ✕ Fuera de alcance

No incluye sensores físicos propios (fuente única: Open-Meteo, con el componente streaming simulado por *polling* hacia Kafka, tal como el curso permite). No cubre otras localidades fuera de Juliaca. No calcula pérdidas económicas ni recomienda variedades de cultivo — solo estima el riesgo climático y su momento probable de ocurrencia.
</div>
</div>
