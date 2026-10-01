# Informe TP-Coordinación

## Coordinación entre procesos (SUM / AGGREGATION / JOIN)

El flujo de un cliente termina en un TOP-3 de frutas. Para armarlo, los datos
deben pasar por tres etapas, cada una combinando en paralelo y cerrando con
una barrera:

```
gateway --> SUM (N) --> AGGREGATION (M) --> JOIN (1) --> gateway --> cliente
```

Sobre ese flujo hay tres mecanismos de control:

- **broadcast EOF**: el EOF se reparte a todos los Sum por el exchange de control.
- **barrera EOF**: cada Aggregation espera los N EOF de un cliente antes de
  emitir su top parcial.
- **barrera top**: Join espera los M top parciales antes de emitir el top final.

**Control de EOF:** en el esqueleto, cada Sum consumía el EOF por su misma cola
de datos, así que lo recibía un solo Sum y el resto nunca se enteraba. Las sumas
parciales nunca se terminaban de completar y el sistema se quedaba esperando. La
solución fue separar el EOF del flujo de datos con un exchange dedicado
(`SUM_CONTROL_EXCHANGE`). El Sum que recibe el EOF lo **republica** al exchange,
y todos los Sum están suscritos a él. Cada uno abre su consumo del exchange en
**un hilo aparte** del que consume los datos, con conexiones separadas, porque
Pika no permite que dos hilos usen la misma conexión.

**Agregado de `client_id` en el protocolo interno:** los mensajes solo llevaban
`[fruit, amount]`. Con varios clientes al mismo tiempo, los Sum y los Aggregations
no podían saber a quién pertenecía cada dato y las sumas de sesiones distintas se
mezclaban en el mismo dict. Se agregó `client_id` a los mensajes internal: el
gateway lo genera por sesión y lo incluye en **cada** mensaje que publica. A
partir de ahí, todos los estados acumulados (`amount_by_fruit_by_client`,
`partial_sum_by_client`, `partials_by_client`) son diccionarios indexados por
`client_id` y se limpian al completar la sesión.

**Protección de la sección crítica:** cada Sum acumula el subtotal en un dict
compartido entre dos hilos: el de datos escribe y el de control (que dispara
`_process_eof`) lo lee y vacía. Ese acceso está protegido con un `Lock`, y
`pop` se hace **dentro** del lock para que el vaciado y la lectura sean atómicos
respecto del otro hilo. Sin esto, un EOF podía vaciar el dict mientras el hilo
de datos seguía escribiendo, y se perdían cantidades.

**Distribución de los datos:** el gateway publica todos los registros en una
sola cola compartida y **todos los Sum la consumen**. RabbitMQ reparte mensaje por
mensaje entre las N instancias, así que la carga se equilibra sola y cada Sum
guarda un subtotal parcial de todos los clientes.

Para la distribución desde los Sum hacia los Aggregators se utiliza `crc32 % M`,
de modo que una misma fruta no vaya a más de un agregador.

**Escalabilidad:** horizontalmente se agregan más instancias de Sum o Aggregation
cambiando `SUM_AMOUNT` / `AGGREGATION_AMOUNT` y los `ID` correspondientes en la
configuración de docker, sin tocar código.

**Cierre ordenado y manejo de SIGTERM:**

- A nivel middleware se utiliza `stop_consuming_threadsafe()` que usa
  `add_callback_threadsafe` de Pika. Esto es necesario porque no es posible llamar
  `stop_consuming()` desde un hilo que no es dueño de la conexión.
- El hilo de control de Sum no es daemon y se hace `join()` antes de cerrar
  conexiones, para que el proceso no termine sin limpiar.
- El orden de cierre es inverso al de arranque: se detiene el consumo, se
  espera a los hilos, después se cierran los canales en el MDW.

**Barreras:**

- **Aggregation** cuenta cuántos EOF recibió por `client_id`. Cuando le
  llegaron `N` (uno de cada Sum), tiene la parcial completa de ese cliente y
  emite un top parcial. Sin esa cuenta, un Aggregation rápido publicaría su
  top con la mitad de los datos.
- **Join** cuenta cuántos top parciales recibió por `client_id`. Cuando le
  llegaron `M` (uno de cada Aggregation), combina las parciales y emite el
  top final.

## Cambios en el middleware (respecto a TP-MOM):

**Colas nombradas en el exchange:**

Ante el problema encontrado de la pérdida de mensajes EOF cuando todavía un Sum
no había declarado la cola, los tests fallaban de forma intermitente.

Se agregó un parámetro `queue_name` al constructor de
`MessageMiddlewareExchangeRabbitMQ`: si se pasa, la cola se declara en el
`__init__` (con `durable=True`) en vez de en `start_consuming`.
Cada Sum declara **las N colas de control**, no solo la propia: al arrancar,
`SumFilter` recorre `range(SUM_AMOUNT)` y declara `sum_i_control` para todos
los `i`. Así el exchange tiene destino para cualquier EOF desde el primer
segundo, haya o no un Sum conectado en ese instante. La lógica de borrado
también se condicionó: la cola se borra al cerrar solo si es anónima, porque
una cola con nombre la comparten todos los Sum.

**Prefetch:**

`PREFETCH_COUNT` se setea en 1.

Con `prefetch > 1` el broker entrega N mensajes al consumidor sin esperar el ACK
de los anteriores, y esos quedan bufferizados en el proceso. El EOF en cambio
viaja por otra conexión, la del exchange de control, así que puede ser procesados
antes que los datos que ya fueron entregados pero todavía no se procesaron.
Cuando `_process_eof` hace `pop` del diccionario y ya manda la parcial, esos
datos pendientes se quedan en un diccionario nuevo que nunca más se envía,
porque el EOF de ese cliente ya pasó. Esto generaba un error silencioso que 
afecta el resultado final del top.
