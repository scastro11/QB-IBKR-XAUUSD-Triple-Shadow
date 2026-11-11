# =============================================================================
#  telemetria_ibkr.py  ·  Operaciones Reales
#
#  La capa de telemetria para Interactive Brokers, sola. Sin estrategia.
#
#  La metes en tu bot y tu bot empieza a auditarse. No cambia una sola decision
#  de trading: solo cuenta lo que hace.
#
#  POR QUE ESTO Y NO EL BOT ENTERO:
#
#  Porque si te doy un robot terminado, te convierto en lo que este curso existe
#  para que NO seas: alguien que corre un bot que no escribio, no entiende y no
#  puede auditar. La estrategia la escribes tu. Esto es lo que hace que puedas
#  comprobar si hace lo que crees que hace.
#
#  QUE NECESITAS: Python 3.9+ y nada mas. Ni una dependencia. Se usa urllib de
#  la libreria estandar a proposito: este fichero tiene que poder entrar en tu
#  proyecto sin tocarte el entorno ni pelearse con tus versiones.
#
#  CON QUE FUNCIONA: con ib_async (el fork MANTENIDO de ib_insync, que quedo
#  huerfano), con ib_insync si ya lo tienes, con la ibapi cruda, o con cualquier
#  otra cosa. Este modulo NO importa ninguna libreria de IBKR — tu le
#  pasas los numeros, el los manda. Asi no se te rompe cuando cambies de libreria.
#
#  Y hay una razon mas dura para leerlo antes de copiarlo: cada bloque de aqui
#  se pago con un fallo real, en MetaTrader o en IBKR. Estan comentados.
# =============================================================================

from __future__ import annotations

import atexit
import json
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

# ============================ LO QUE TIENES QUE PONER ========================
# El token sale de la app: Agentes -> tu agente -> "Conexion". Es personal e
# intransferible, y es lo unico que decide de quien es cada evento.
#
# NO LO ESCRIBAS AQUI DENTRO SI EL FICHERO VA A UN REPOSITORIO. Ponlo en una
# variable de entorno. Un token en un commit publico es un agente que cualquiera
# puede ensuciar con datos falsos, y este panel solo vale lo que valga su dato.
TOKEN = os.environ.get("OPERACIONES_REALES_TOKEN", "PEGA_AQUI_TU_TOKEN")
# The private ingestion endpoint is deliberately excluded from this public
# repository. Set it locally only when you are authorized to use that service.
URL = os.environ.get("OPERACIONES_REALES_URL", "").strip()

# A diferencia de MetaTrader, aqui NO hay lista blanca de URLs que autorizar.
# Python sale a internet sin pedirle permiso a nadie. Todo el capitulo de
# "err=4014" que se lleva la tarde de medio curso, en IBKR sencillamente no
# existe. Lo que si existe son OTRAS cuatro trampas, y estan mas abajo.

# Cada cuanto late el pulso. 15 minutos es lo que espera el panel: por debajo de
# eso solo gastas peticiones, por encima el panel te da por muerto sin estarlo.
PULSO_SEG = 15 * 60

# Cuantos segundos espera el modulo antes de anunciarse (evento START).
# NO es una mania: ver TRAMPA 1.
ANUNCIO_SEG = 5

# Techo de la cola. Si tu bot genera mas eventos de los que la red puede tragar,
# preferimos perder los VIEJOS y no los nuevos, y decirlo por consola. Una cola
# sin techo es una fuga de memoria con buenos modales.
COLA_MAX = 500

# Donde caen los eventos que no se pudieron entregar. Ver TRAMPA 4.
PENDIENTES = "telemetria_pendientes.jsonl"


# ============================== UTILIDADES ===================================

def es_paper(cuenta_ib: str) -> bool:
    """Es una cuenta de papel de IBKR?

    Las cuentas paper empiezan por DU (individual) o DF (family/advisor). Las
    reales empiezan por U o F. Es la unica forma fiable de saberlo sin
    preguntarselo al usuario — y preguntarselo seria pedirle que declare el dato
    mas importante de la auditoria, que es justo lo que aqui no se hace.
    """
    return cuenta_ib.upper().startswith(("DU", "DF"))


def cuenta_num(cuenta_ib: str) -> int | None:
    """El numero de cuenta, sin las letras.

    El panel guarda `account` como ENTERO (viene de MetaTrader, donde la cuenta
    es un numero). "DU1234567" entero no es, asi que se manda 1234567 y el
    identificador completo viaja en la foto del entorno. Si mandaramos el texto
    tal cual, el servidor lo convertiria a nulo EN SILENCIO y te quedarias sin
    selector de cuenta sin que nadie te dijera por que.
    """
    digitos = "".join(c for c in cuenta_ib if c.isdigit())
    return int(digitos) if digitos else None


def spread_usd(bid: float, ask: float, multiplicador: float, cantidad: float) -> float | None:
    """Lo que te costo el spread de ESTA entrada, EN DINERO.

    Eres el unico que puede calcularlo: el servidor no sabe el multiplicador de
    tu contrato. Acciones -> 1. MES (micro e-mini S&P) -> 5. MNQ -> 2. MGC -> 10.
    Opciones -> 100. Si no lo sabes, NO lo adivines: devuelve None y el panel
    dira "no lo sabemos", que es verdad. Un cero diria "no hubo spread", que es
    mentira — y una mentira plausible no la caza nadie.
    """
    if bid is None or ask is None or ask <= 0 or bid <= 0:
        return None
    return abs(ask - bid) * multiplicador * abs(cantidad)


def equity_de_agente(
    capital_asignado: float,
    realizado: float,
    flotante: float = 0.0,
) -> float:
    """El equity de TU agente cuando compartes cuenta con otros bots.

    Existe por TRAMPA 5: `NetLiquidation` es de la cuenta entera, asi que dos
    agentes sobre la misma cuenta paper reportan la MISMA curva. Este es el
    numero que si es tuyo:

        capital_asignado : el trozo de cuenta que le asignaste a este bot. Lo
                           decides tu, no la API. Es la linea de salida de la
                           curva; si te la inventas, el drawdown en % sale mal.
        realizado        : la suma de los PnL de TUS cierres desde que arranco.
        flotante         : el PnL no realizado de TUS posiciones abiertas.

    No consulta nada: suma lo que le das. El unico que sabe que posiciones son
    de este bot eres tu — IBKR no etiqueta la posicion con el clientId.
    """
    return round(float(capital_asignado) + float(realizado) + float(flotante), 2)


def _hoy_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _contexto_seguro(v: Any) -> dict[str, Any] | None:
    """Sanea el contexto ANTES de que salga de aquí.

    ESTO ARREGLA EL PEOR FALLO QUE TUVO ESTE MÓDULO, y merece la pena contarlo entero
    porque es la enfermedad que este producto existe para curar, cometida por él mismo.

    `contexto` es el único campo libre del contrato: tú metes ahí lo que tu robot miraba al
    entrar. Y si metías un valor que json.dumps no sabe serializar —un set, un objeto de
    ib_insync, un Decimal, un numpy.float64— la excepción saltaba DENTRO del hilo de envío.
    El hilo se moría. Y a partir de ese instante no se enviaba NADA MÁS, nunca, sin un solo
    aviso en el panel: el agente se quedaba en verde (porque "verde" significa "mandó algo
    hace poco") sobre un robot que ya no reportaba.

    Un bug de telemetría que apaga la telemetría en silencio es exactamente lo que esta
    plataforma existe para cazar.

    Se aplican las MISMAS reglas que el servidor: máximo 24 claves, solo valores simples.
    Lo que no sea un escalar se convierte a texto en vez de reventar — porque perder el
    detalle de un campo opcional es barato, y perder toda tu telemetría no.
    """
    if v is None:
        return None
    if not isinstance(v, dict):
        return None
    limpio: dict[str, Any] = {}
    for k, val in v.items():
        if len(limpio) >= 24:
            break
        clave = str(k)[:32]
        if val is None or isinstance(val, (int, float, bool)):
            limpio[clave] = val
        else:
            # Todo lo demás (set, Decimal, numpy, un objeto de tu librería) se degrada a
            # texto recortado. Es lo que el servidor haría con un string, y no explota.
            limpio[clave] = str(val)[:64]
    return limpio or None


def _limpio(d: dict[str, Any]) -> dict[str, Any]:
    """Quita las claves en None.

    NO es cosmetica. El servidor solo pisa la foto del entorno con los campos
    que llegan: mandar None borraria un dato bueno que ya estaba. Y en los
    eventos, un campo ausente y un campo en nulo significan lo mismo — "no lo
    se" — pero el cuerpo pesa la mitad.
    """
    return {k: v for k, v in d.items() if v is not None}


# ============================== EL MODULO ====================================

class Telemetria:
    """La capa de telemetria. Se instancia una vez por bot.

    Un bot = un agente = un token = un clientId. Si corres tres estrategias
    sobre la misma cuenta de IBKR, son tres agentes con tres tokens: mezclarlas
    bajo uno solo hace que la curva de equity que veas no sea de ninguna de las
    tres.
    """

    def __init__(
        self,
        token: str = TOKEN,
        *,
        cuenta_ib: str,
        client_id: int,
        version: str,
        simbolo: str,
        host: str = "127.0.0.1",
        puerto: int = 7497,
        terminal: str = "TWS",          # "TWS" o "IB Gateway"
        timeframe: str | None = None,   # el marco de tu estrategia, ej. "5 mins"
        moneda: str | None = None,      # moneda base de la cuenta, ej. "USD"
        solo_lectura: bool = False,     # API en modo Read-Only? Ver _entorno()
        url: str = URL,
        pendientes: str = PENDIENTES,
    ):
        self.token = token
        self.url = url
        self.cuenta_ib = cuenta_ib
        self.client_id = client_id
        # 16 caracteres: es lo que acepta el servidor. Mas largo se trunca, y tu
        # "1.4.2-experimento-lunes" acaba siendo "1.4.2-experiment".
        self.version = version[:16]
        self.simbolo = simbolo[:32]
        self.host = host
        self.puerto = puerto
        self.terminal = terminal
        self.timeframe = timeframe
        self.moneda = moneda
        self.solo_lectura = solo_lectura
        self.pendientes = pendientes
        self._conectado = True

        # -------------------- ESTADO INTERNO --------------------
        # TODO esto se resetea en iniciar(). Ver TRAMPA 3: no es mania, es el
        # bug que inunda la plataforma.
        self._anunciado = False
        self._dia_latido = ""
        self._pico_equity: float | None = None
        self._equity: float | None = None
        self._balance: float | None = None
        self._muriendo = False

        self._cola: queue.Queue = queue.Queue(maxsize=COLA_MAX)
        self._hilo: threading.Thread | None = None
        self._parar = threading.Event()
        self._ultimo_pulso = 0.0
        # Para no convertir un aviso en un diluvio. Ver _encolar().
        self._descartados = 0
        self._ultimo_aviso_cola = 0.0

    # ---------------------------------------------------------------------
    # TRAMPA 1 — NO TE ANUNCIES AL CONECTAR, Y TAMPOCO ESPERES AL PRIMER TRADE.
    #
    # En MetaTrader el motivo es que la red del terminal puede no estar lista.
    # En IBKR es peor y menos evidente: cuando connect() devuelve, TWS TODAVIA
    # NO TE HA MANDADO LA CUENTA. `managedAccounts` llega despues, por callback.
    # Si te anuncias en la linea siguiente al connect, mandas un START con la
    # cuenta vacia — y el panel te pinta un agente sin cuenta, sin moneda y sin
    # saber si es paper o real. Justo el dato que hace falta para creerse el resto.
    #
    # Y esperar al primer trade tampoco vale: un domingo, con el mercado cerrado,
    # no sabrias si tu integracion quedo bien hasta el lunes.
    #
    # Por eso: temporizador. Arrancas, pasan ANUNCIO_SEG, se manda el START.
    # ---------------------------------------------------------------------
    def iniciar(self) -> None:
        """Arranca el hilo de envio y programa el anuncio.

        Llamalo DESPUES de conectar con TWS/Gateway y de tener la cuenta.
        """
        self._anunciado = False
        self._dia_latido = ""
        self._muriendo = False
        self._parar.clear()
        self._ultimo_pulso = time.time()

        if self._hilo is None or not self._hilo.is_alive():
            self._hilo = threading.Thread(
                target=self._worker, name="telemetria", daemon=True
            )
            self._hilo.start()

        self._programar_anuncio()
        atexit.register(self._despedida_final)

    def _programar_anuncio(self) -> None:
        # EL TEMPORIZADOR VA EN MODO DEMONIO, Y NO ES UN DETALLE.
        #
        # Un threading.Timer normal NO es demonio: Python espera a que termine antes de
        # cerrar el proceso. O sea que un Ctrl+C se quedaba colgado hasta ANUNCIO_SEG
        # segundos, sin decir nada, mientras esperaba a un temporizador que —si ya nos
        # habiamos despedido— no iba a hacer absolutamente nada al dispararse.
        #
        # Cinco segundos de proceso que no muere es la clase de detalle que hace que alguien
        # mate el bot con kill -9. Y un bot muerto a la fuerza no manda su despedida, asi que
        # el panel se queda diciendo "vivo". El fallo pequeno se paga en el dato.
        t = threading.Timer(ANUNCIO_SEG, self._anunciar)
        t.daemon = True
        t.start()

    def _anunciar(self) -> None:
        if self._anunciado or self._parar.is_set():
            return
        self._anunciado = True
        self._encolar("START", self._entorno())

    # ---------------------------------------------------------------------
    # TRAMPA 2 — LA TELEMETRIA NUNCA BLOQUEA LA OPERATIVA.
    #
    # Es la regla que mas dinero salva de todo el fichero, y en Python muerde
    # IGUAL que en MQL5 aunque por otro camino.
    #
    # ib_async (y su predecesor ib_insync) corre sobre asyncio EN EL HILO
    # PRINCIPAL. Un POST sincrono de
    # dos segundos dentro de un callback (onFill, onOrderStatus, onBarUpdate) no
    # "va lento": PARA EL BUCLE DE EVENTOS. Mientras dura, tu bot no ve ticks,
    # no procesa el estado de las ordenes y no mueve un trailing. Un timeout en
    # mitad de una gestion es una perdida de dinero REAL, no un problema de
    # telemetria.
    #
    # Por eso todo sale por una cola a un hilo aparte, y encolar NUNCA bloquea:
    # si la cola esta llena se tira el evento MAS VIEJO. Perder un latido de
    # hace diez minutos es gratis; congelar el hilo que gestiona una posicion
    # viva, no.
    # ---------------------------------------------------------------------
    def _encolar(self, evento: str, extra: dict[str, Any] | None = None) -> None:
        cuerpo = self._cuerpo(evento, extra or {})
        try:
            self._cola.put_nowait(cuerpo)
        except queue.Full:
            try:
                self._cola.get_nowait()          # tira el mas viejo
                self._cola.put_nowait(cuerpo)
            except queue.Empty:
                return
            # EL AVISO SE AGRUPA, Y NO ES UN DETALLE DE ESTILO.
            #
            # La cola se llena cuando la red esta caida, y entonces se llena en
            # CADA evento: un print por descarte convierte un aviso en un
            # diluvio de miles de lineas que tapa el log de TU bot — justo el
            # sitio donde vas a buscar por que no operaste. Un aviso que hay que
            # apagar para poder trabajar no es un aviso.
            #
            # Se cuenta todo y se dice una vez por minuto, con el total.
            self._descartados += 1
            ahora = time.time()
            if ahora - self._ultimo_aviso_cola >= 60:
                self._ultimo_aviso_cola = ahora
                print("[telemetria] cola llena: {} eventos descartados (los mas antiguos). "
                      "La red no traga; los que fallen del todo caen en {}."
                      .format(self._descartados, self.pendientes))

    def _cuerpo(self, evento: str, extra: dict[str, Any]) -> dict[str, Any]:
        base = {
            "event": evento,
            "account": cuenta_num(self.cuenta_ib),
            "symbol": self.simbolo,
            # `magic` es de MetaTrader: "que EA abrio esto". En IBKR el
            # equivalente exacto es el clientId, y ademas es un entero. Se mapea
            # a proposito y se declara aqui: es lo que te deja distinguir tres
            # bots sobre la misma cuenta. No es un dato inventado — es el mismo
            # dato con otro nombre.
            "magic": self.client_id,
            "ea_version": self.version,
            "equity": self._equity,
            "balance": self._balance,
            "peak_equity": self._pico_equity,
            "dd_pct": self._dd_pct(),
        }
        base.update(extra)
        return _limpio(base)

    def _dd_pct(self) -> float | None:
        if self._equity is None or not self._pico_equity:
            return None
        return round((self._pico_equity - self._equity) / self._pico_equity * 100.0, 4)

    def _entorno(self) -> dict[str, Any]:
        """La foto de DONDE corre el agente. Viaja con el anuncio y con el latido.

        Es lo que permite contestar "quedo bien conectado?" EN EL ACTO, sin
        esperar a que abra el mercado.

        HONESTIDAD SOBRE LO QUE NO SE PUEDE SABER, que es la mitad del valor de
        esta funcion:
          - terminal_vps: MetaTrader tiene una senal para esto; IBKR no tiene
            ninguna. Corras en tu portatil o en un VPS, TWS no lo sabe. Va en
            nulo. Inventarlo seria fabricar el dato.
          - leverage: en IBKR no hay un apalancamiento fijo de cuenta, hay margen
            por producto. Va en nulo porque el concepto no existe, no porque no
            lo hayamos buscado.
          - ping_ms: es la latencia con el broker, y solo la sabes si la mides tu
            (reqCurrentTime). Pasala si la tienes.
        """
        return _limpio({
            "terminal_name": self.terminal[:64],
            "broker_company": "Interactive Brokers",
            # El identificador COMPLETO de la cuenta vive aqui, porque en
            # `account` solo caben los digitos.
            "broker_server": "{} @ {}:{}".format(self.cuenta_ib, self.host, self.puerto)[:64],
            "account_type": "demo" if es_paper(self.cuenta_ib) else "real",
            "account_currency": self.moneda,
            "timeframe": self.timeframe,
            "connected": self._conectado,
            # En IBKR "puedo operar" no es un interruptor del terminal como el
            # AutoTrading de MetaTrader: si la API acepta la conexion, operas.
            # PERO existe el modo Read-Only API (Configuracion > API), y ese SI
            # es la mentira cara: panel verde sobre un bot que no puede enviar
            # ni una orden. Si lo tienes puesto, dilo al construir el modulo.
            "trade_allowed": not self.solo_lectura,
        })

    # ---------------------------------------------------------------------
    # EL HILO QUE MANDA. Y TRAMPA 4, dentro.
    # ---------------------------------------------------------------------
    def _worker(self) -> None:
        # EL HILO DE ENVIO NO SE MUERE. NUNCA. PASE LO QUE PASE.
        #
        # Sin este try/except, CUALQUIER excepcion inesperada aqui dentro mata el hilo — y a
        # partir de ese momento no se manda nada mas, para siempre, sin un solo aviso. El panel
        # se queda en verde (porque "verde" significa "mando algo hace poco") sobre un robot que
        # dejo de reportar hace horas.
        #
        # Eso es LA MISMA MENTIRA que este producto existe para cazar, cometida por el propio
        # producto. Un fallo en la telemetria puede costarte un evento; jamas puede costarte la
        # telemetria entera. Se registra, y se sigue.
        while True:
            try:
                try:
                    item = self._cola.get(timeout=1.0)
                except queue.Empty:
                    item = None

                if item is not None:
                    if item.get("event") == "__fin__":
                        self._cola.task_done()
                        return
                    self._entregar(item)
                    self._cola.task_done()

                # EL PULSO. Cada 15 minutos, y NO ENTRA EN TU HISTORIAL: solo dice
                # "sigo aqui". Sin esto, el panel no puede distinguir un bot vivo de
                # uno que se cayo hace veinte horas — porque "vivo" solo significaba
                # "mando algo hace poco", y el latido es diario.
                #
                # Va en ESTE hilo a proposito: en el hilo de trading serian 96
                # llamadas de red al dia metiendose en tu bucle de eventos por nada.
                if not self._parar.is_set() and time.time() - self._ultimo_pulso >= PULSO_SEG:
                    self._ultimo_pulso = time.time()
                    self._entregar({
                        "event": "PING",
                        "connected": self._conectado,
                        "trade_allowed": not self.solo_lectura,
                    })
            except Exception as e:
                print("[telemetria] error inesperado en el hilo de envio ({}). Sigo vivo.".format(e))
                time.sleep(1)
                continue

    def _entregar(self, cuerpo: dict[str, Any], reintentos: int = 3) -> bool:
        datos = json.dumps(cuerpo).encode("utf-8")
        espera = 1.0
        for intento in range(reintentos):
            req = urllib.request.Request(
                self.url,
                data=datos,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "x-runner-token": self.token,
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    if r.status in (200, 201):
                        return True
                    print("[telemetria] respuesta inesperada {}".format(r.status))
            except urllib.error.HTTPError as e:
                # 401 = token malo, o agente en la papelera. Reintentar no lo
                # arregla nunca: se dice y se corta. Un bucle de reintentos sobre
                # un 401 solo consigue que el error se pierda entre el ruido.
                if e.code in (400, 401, 413):
                    detalle = e.read()[:200]
                    print("[telemetria] HTTP {}: {} - revisa el token y el cuerpo".format(e.code, detalle))
                    return False
                print("[telemetria] HTTP {}, reintento {}/{}".format(e.code, intento + 1, reintentos))
            except Exception as e:                       # red caida, DNS, timeout
                print("[telemetria] fallo de red ({}), reintento {}/{}".format(e, intento + 1, reintentos))
            time.sleep(espera)
            espera *= 2

        # ---------------------------------------------------------------
        # TRAMPA 4 — LO QUE NO SE ENTREGA, NO SE PIERDE.
        #
        # Un EA de MetaTrader que no puede mandar un CLOSE lo pierde para
        # siempre: no tiene donde guardarlo. Python si tiene disco, asi que aqui
        # no hay excusa. El evento cae a un .jsonl al lado del bot y se reenvia
        # despues con reenviar_pendientes().
        #
        # Importa mas de lo que parece: los fallos de red no son aleatorios. Se
        # concentran justo cuando el mercado se mueve — que es cuando tu bot
        # opera. Perder "los eventos de los dias malos" sesga la muestra hacia
        # arriba, y una auditoria sesgada hacia arriba es peor que ninguna.
        # ---------------------------------------------------------------
        try:
            with open(self.pendientes, "a", encoding="utf-8") as f:
                f.write(json.dumps(cuerpo, ensure_ascii=False) + "\n")
            print("[telemetria] evento guardado en {} para reenviar".format(self.pendientes))
        except Exception as e:
            print("[telemetria] no se pudo guardar el pendiente: {}".format(e))
        return False

    def reenviar_pendientes(self) -> int:
        """Reenvia lo que quedo en el fichero de pendientes.

        Devuelve cuantos entraron. Los que sigan fallando se quedan en el
        fichero. Llamalo al arrancar, despues de iniciar().
        """
        if not os.path.exists(self.pendientes):
            return 0
        with open(self.pendientes, encoding="utf-8") as f:
            lineas = [ln for ln in f.read().splitlines() if ln.strip()]
        quedan: list[str] = []
        ok = 0
        for ln in lineas:
            try:
                cuerpo = json.loads(ln)
            except json.JSONDecodeError:
                continue
            if self._entregar(cuerpo, reintentos=1):
                ok += 1
            else:
                quedan.append(ln)
        with open(self.pendientes, "w", encoding="utf-8") as f:
            f.write("\n".join(quedan) + ("\n" if quedan else ""))
        return ok

    # ---------------------------------------------------------------------
    # LO QUE LLAMAS DESDE TU BOT
    # ---------------------------------------------------------------------
    # ---------------------------------------------------------------------
    # TRAMPA 5 — `NetLiquidation` ES DE LA CUENTA ENTERA, NO DE TU AGENTE.
    #
    # Lo cazo un alumno leyendo el ejemplo, y tiene toda la razon. El ejemplo de
    # este fichero hace lo obvio:
    #
    #     if v.tag == "NetLiquidation": tl.cuenta(equity=float(v.value))
    #
    # y ese numero es el patrimonio liquido de LA CUENTA. En MetaTrader eso casi
    # nunca molestaba porque la practica normal es una cuenta por EA. En IBKR es
    # al reves: lo normal es una sola cuenta paper y varios clientId encima.
    #
    # Consecuencia exacta: DOS AGENTES SOBRE LA MISMA CUENTA REPORTAN LA MISMA
    # CURVA DE EQUITY. Y no es que se parezcan: es el MISMO numero, punto por
    # punto. El panel te ensena dos sistemas y los dos suben cuando sube el otro.
    # Peor todavia, el drawdown de cada agente pasa a ser el de la cuenta, asi
    # que un agente impecable hereda el hundimiento del que va mal — y si tienes
    # un cortacircuitos por drawdown, salta por culpa de un bot que no es el suyo.
    #
    # No hay forma de que el modulo lo arregle solo: el servidor recibe un numero
    # y no puede saber de quien es. Lo tienes que separar TU, y hay dos maneras.
    #
    # LA BUENA, si te la puedes permitir: UNA SUBCUENTA POR AGENTE. En IBKR es
    # gratis abrir cuentas paper adicionales, y en real existen las subcuentas.
    # Un agente = una cuenta = NetLiquidation ya significa lo que crees.
    #
    # LA QUE VAS A USAR EN PAPER: reconstruye TU equity. El unico que sabe que
    # posiciones son tuyas eres tu — IBKR NO etiqueta la posicion con el
    # clientId que la abrio (el permId esta en la ORDEN, no en la posicion
    # agregada). O sea que no lo puedes deducir de la API: lo tienes que llevar
    # apuntado en tu bot.
    #
    #     equity_del_agente = capital_asignado + realizado_tuyo + flotante_tuyo
    #
    # `equity_de_agente()` (justo debajo) hace esa suma. Y si eliges esto,
    # ELIGELO ENTERO: no mezcles: o mandas siempre el NetLiquidation de la cuenta
    # —y entonces sabes que la curva es de la cuenta, no del agente— o mandas
    # siempre el tuyo. Alternar los dos produce una curva que no es de nadie.
    #
    # LO QUE NO SIRVE: `reqPnL(cuenta)` tampoco. Devuelve dailyPnL, unrealizedPnL
    # y realizedPnL de LA CUENTA. Es el mismo problema con otro nombre.
    # ---------------------------------------------------------------------
    def cuenta(self, equity: float, balance: float | None = None) -> None:
        """Actualiza equity y balance.

        Llamalo cuando IBKR te los refresque (accountSummary / accountValue).
        El pico de equity se lleva solo.

        OJO: si le pasas `NetLiquidation` tal cual y hay MAS DE UN BOT sobre esta
        cuenta, esta curva es de la CUENTA, no de este agente. Lee TRAMPA 5 aqui
        arriba antes de decidir que numero mandas.
        """
        self._equity = equity
        self._balance = balance if balance is not None else equity
        if self._pico_equity is None or equity > self._pico_equity:
            self._pico_equity = equity

    def conexion(self, conectada: bool) -> None:
        """Actualiza el estado que publican PING, START y HEARTBEAT."""
        self._conectado = bool(conectada)

    def abrir(
        self,
        *,
        direccion: str,                  # "BUY" o "SELL"
        lote: float,                     # cantidad: acciones, contratos...
        precio_pedido: float | None = None,
        precio_ejecutado: float | None = None,
        spread_pts: float | None = None,
        spread_dinero: float | None = None,
        riesgo_pct: float | None = None,
        atr_entrada: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        perm_id: int | None = None,
        contexto: dict[str, Any] | None = None,
    ) -> None:
        """La apertura.

        `perm_id` es el permId de IBKR, no el orderId. El orderId cambia entre
        sesiones; el permId no. Es lo que permite que una importacion de
        historial no te duplique las operaciones.
        """
        slip = None
        if precio_pedido is not None and precio_ejecutado is not None:
            slip = round(precio_ejecutado - precio_pedido, 6)
        self._encolar("OPEN", {
            "dir": direccion,
            "lot": lote,
            "intended": precio_pedido,
            "filled": precio_ejecutado,
            "slip_pts": slip,
            "spread_pts": spread_pts,
            "spread_usd": spread_dinero,
            "risk_pct": riesgo_pct,
            "entry_atr": atr_entrada,
            "sl_price": sl,
            "tp_price": tp,
            "position_id": perm_id,
            "contexto": _contexto_seguro(contexto),
            # `retcode` es el codigo de OrderSend de MetaTrader. En IBKR no
            # existe: el estado de la orden es texto ("Filled", "Cancelled").
            # Va en nulo. Meter un 10009 para que "quede bonito" seria
            # falsificar una confirmacion de broker.
        })

    def cerrar(
        self,
        *,
        direccion: str,
        resultado: float,                # PnL realizado
        comision: float | None = None,
        motivo: str | None = None,       # TP|SL|TRAIL|BREAKEVEN|CIRCUIT|MANUAL|TIME|OTHER
        precio_pedido: float | None = None,
        precio_ejecutado: float | None = None,
        mfe_atr: float | None = None,
        salida_atr: float | None = None,
        duracion_seg: int | None = None,
        perm_id: int | None = None,
    ) -> None:
        """El cierre.

        DOS AVISOS QUE VALEN DINERO, y los dos son de IBKR, no nuestros:

        1. LA COMISION LLEGA TARDE, Y AL LEERLA PRONTO NO SALE VACIA: SALE CERO.
           IBKR te avisa del fill en `execDetails` y te manda la comision DESPUES,
           en `commissionReport`, en otro callback.

           MEDIDO EN UNA CUENTA PAPER DE VERDAD (SPY, 25-ago-2026): en el instante
           del fill, `fill.commissionReport.commission` valia **0.0**. El callback
           con el valor real —1.000003 USD— llego 2 ms despues.

           Dos milisegundos. Por eso este fallo no se ve probando: se ve en la
           factura. Y fijate en lo que de verdad muerde, que no es la tardanza:
           un CERO NO ES UN HUECO. Un nulo dice "no lo se"; un cero dice "lo medi
           y no hubo comision". Si emites el CLOSE al ver el fill, no te quedas
           sin el dato — te llevas un dato FALSO, y ademas plausible, que dice
           que tu sistema no paga costes. El panel te lo cree, tu profit factor
           sube, y no hay forma de cazarlo mirando la pantalla.

           MANDA EL CLOSE DESDE `commissionReport`, NO DESDE EL FILL.

        2. `swap` NO EXISTE en IBKR, y no es cero. El interes de financiacion se
           cobra a fin de mes sobre el saldo de la cuenta, no por posicion: no se
           puede repartir por trade sin inventarselo. Por eso este metodo no
           tiene parametro de swap — el campo se queda en nulo, que significa "no
           lo sabemos". Un cero significaria "no lo hubo".

        Y UNA COMPROBACION QUE TIENES QUE HACER UNA VEZ: mira si el
        `realizedPNL` que te da tu commissionReport ya viene neto de esa comision
        o no, contrastandolo con UN trade de tu extracto de IBKR. Segun producto
        y version de la API, no siempre es lo mismo. Si viene neto y ademas
        mandas la comision aparte, el panel te la restara dos veces. Compruebalo
        con un trade y sigue tranquilo; no lo compruebes y arrastraras el sesgo
        en todas tus metricas sin verlo nunca.
        """
        slip = None
        if precio_pedido is not None and precio_ejecutado is not None:
            slip = round(precio_ejecutado - precio_pedido, 6)
        self._encolar("CLOSE", {
            "dir": direccion,
            "profit": resultado,
            "commission": comision,
            "exit_reason": motivo,
            "intended": precio_pedido,
            "filled": precio_ejecutado,
            "slip_pts": slip,
            "mfe_atr": mfe_atr,
            "exit_atr": salida_atr,
            "duracion_seg": duracion_seg,
            "position_id": perm_id,
        })

    def latido(
        self,
        *,
        senales: int = 0,
        entradas: int = 0,
        bloq_spread: int = 0,
        bloq_riesgo: int = 0,
        bloq_circuito: int = 0,
        flotante: float | None = None,
        abiertas: int | None = None,
        contexto: dict[str, Any] | None = None,
    ) -> None:
        """El latido DIARIO.

        Llamalo en tu bucle todo lo que quieras: se manda una vez al dia y las
        demas llamadas no hacen nada.

        -------------------------------------------------------------------
        `flotante` y `abiertas`: LO QUE TODAVIA NO HA CERRADO.

        Esto lo pidio un alumno, y la observacion es exacta: "en un sistema
        convexo casi todo el pago vive en una posicion abierta; midiendo solo
        cierres se ve degradandose justo mientras funciona".

        Es literalmente cierto. Una piramide que va sumando niveles paga
        comisiones y cierra los intentos fallidos —cierres pequenos y en rojo,
        uno detras de otro— mientras el pago entero se acumula en la posicion
        que sigue viva. Un panel que solo cuenta cierres pinta la mejor racha
        de su vida como una curva descendente. Y quien mira ese panel apaga el
        sistema justo el dia que empezo a funcionar.

        La curva de equity YA lo recoge (NetLiquidation incluye el no
        realizado), pero la curva no te dice CUANTO de lo que ves esta todavia
        en el aire ni sobre cuantas posiciones. Estos dos numeros si.

        Van dentro de `contexto` a proposito: no son un campo del contrato, y
        NO inventamos uno. Aparecen tal cual en la exportacion (CSV lleva la
        columna cruda, JSON lleva la fila entera) y son tuyos para analizar.

          flotante -> PnL NO REALIZADO de TUS posiciones, en dinero.
          abiertas -> cuantas posiciones tienes vivas ahora mismo.

        Si no lo sabes, no lo mandes. `None` dice "no lo se"; un 0 dice "lo
        medi y no tengo nada abierto", que es una afirmacion muy distinta.
        -------------------------------------------------------------------
        """
        hoy = _hoy_utc()
        if hoy == self._dia_latido:
            return
        self._dia_latido = hoy
        extra: dict[str, Any] = {
            "hb_day": hoy,
            "signals": senales,
            "entries": entradas,
            "blk_spread": bloq_spread,
            "blk_risk": bloq_riesgo,
            "blk_circuit": bloq_circuito,
        }
        exposicion = dict(contexto) if contexto else {}
        if flotante is not None:
            exposicion["flotante_usd"] = round(float(flotante), 2)
        if abiertas is not None:
            exposicion["posiciones_abiertas"] = int(abiertas)
        ctx = _contexto_seguro(exposicion)
        if ctx:
            extra["contexto"] = ctx
        extra.update(self._entorno())
        self._encolar("HEARTBEAT", extra)

    def corte_por_drawdown(self, dd_pct: float) -> None:
        """Tu circuit breaker corto por drawdown."""
        self._encolar("DD_STOP", {"dd_pct": dd_pct})

    def bloqueo_de_riesgo(self, motivo: str) -> None:
        """Tu gestion de riesgo bloqueo una entrada."""
        self._encolar("RISK_GUARD", {"payload": motivo[:500]})

    def descuadre(self, detalle: str) -> None:
        """Lo que tu bot cree que tiene abierto NO es lo que dice IBKR.

        En MetaTrader esto es raro. En IBKR es el fallo tipico del reinicio
        diario del Gateway: tu proceso sobrevive, la conexion no, y al volver tu
        estado en memoria y las posiciones reales pueden no coincidir. Reconcilia
        contra reqPositions() al reconectar y avisa aqui si no cuadran.
        """
        self._encolar("RECONCILE", {"payload": detalle[:500]})

    # ---------------------------------------------------------------------
    # TRAMPA 3 — RECONECTAR NO ES MORIRSE. Y EN IBKR PASA TODOS LOS DIAS.
    #
    # IB Gateway se reinicia SOLO una vez al dia (es su politica, no un fallo).
    # Tu bot se desconecta y se vuelve a conectar. Eso NO es una muerte: vuelve
    # en segundos.
    #
    # Si mandas STOP en cada desconexion, el panel dice "el agente se retiro"
    # todos los dias a la misma hora sobre un bot que esta perfectamente vivo.
    # Y si ademas no reseteas `_anunciado` al reconectar, pasa la version cara
    # del mismo bug — la que nos inundo la plataforma en MetaTrader: el modulo se
    # salta el anuncio y se pone a mandar latidos en bucle. No se ve en la
    # primera prueba; se ve una semana despues.
    #
    # Por eso hay DOS metodos distintos, y usar el que no toca ES el fallo:
    #   reconectado() -> vuelvo a estar, reseteo mi estado, me re-anuncio.
    #   detener()     -> me voy PARA NO VOLVER (Ctrl+C, fin del proceso).
    # ---------------------------------------------------------------------
    def reconectado(self) -> None:
        """Llamalo cuando tu bot recupere la conexion con TWS/Gateway."""
        if self._parar.is_set():
            return
        self._anunciado = False
        self._programar_anuncio()

    def detener(self, motivo: str = "El bot se detuvo") -> None:
        """La despedida. SOLO cuando el proceso se va de verdad.

        Sin esto, el panel sigue diciendo "vivo" durante horas sobre un bot que
        ya no existe — que es exactamente la enfermedad que este producto cura,
        cometida por el propio producto.
        """
        if self._muriendo:
            return
        self._muriendo = True
        self._parar.set()
        # El STOP se entrega EN DIRECTO, sin pasar por la cola: si el proceso se
        # esta cerrando, el hilo demonio puede morir antes de vaciarla.
        self._entregar(self._cuerpo("STOP", {"payload": motivo[:500]}), reintentos=2)
        try:
            self._cola.put_nowait({"event": "__fin__"})
        except queue.Full:
            pass
        if self._hilo and self._hilo.is_alive():
            self._hilo.join(timeout=5)

    def _despedida_final(self) -> None:
        # atexit corre tambien en un Ctrl+C limpio. Si ya te despediste a mano,
        # esto no hace nada.
        self.detener("El proceso termino")


# =============================================================================
#  EJEMPLO DE CABLEADO CON ib_async
#
#  Esto NO es una estrategia: es el esqueleto de donde va cada llamada. La
#  estrategia la escribes tu.
# =============================================================================
if __name__ == "__main__":
    # ib_async, NO ib_insync. ib_insync es el original y esta HUERFANO: su autor lo
    # archivo. ib_async es el fork mantenido por la comunidad y la API es la MISMA,
    # asi que si tienes codigo viejo solo cambias este import.
    #     pip install ib_async
    from ib_async import IB

    CLIENT_ID = 11        # uno por bot. TWS limita CONEXIONES simultaneas, no bots.
    PUERTO = 7497         # 7497 TWS paper / 7496 TWS real / 4002 Gateway paper / 4001 Gateway real

    ib = IB()
    ib.connect("127.0.0.1", PUERTO, clientId=CLIENT_ID)

    # La cuenta llega por callback. Aqui ya esta porque connect() de ib_async
    # espera a la sincronizacion inicial; con la ibapi cruda tendrias que
    # esperar a managedAccounts() tu mismo. Ver TRAMPA 1.
    cuenta = ib.managedAccounts()[0]

    tl = Telemetria(
        cuenta_ib=cuenta,
        client_id=CLIENT_ID,
        version="1.0.0",
        simbolo="MES",
        puerto=PUERTO,
        terminal="TWS",
        timeframe="5 mins",
        moneda="USD",
    )
    tl.iniciar()
    tl.reenviar_pendientes()

    # -------------------------------------------------------------------------
    # EQUITY. Y AQUI HAY QUE ELEGIR — ver TRAMPA 5.
    #
    # `NetLiquidation` es de la CUENTA ENTERA. Con UN solo bot encima es
    # exactamente lo que quieres. Con DOS, los dos agentes del panel reportan la
    # MISMA curva y el drawdown de cada uno es el de la cuenta.
    #
    # Deja este bloque tal cual si eres el unico bot en esta cuenta. Si no lo
    # eres, comentalo y usa el de abajo.
    # -------------------------------------------------------------------------
    UNICO_BOT_EN_ESTA_CUENTA = True

    def _cuenta_actualizada(v):
        if v.tag == "NetLiquidation" and v.account == cuenta and UNICO_BOT_EN_ESTA_CUENTA:
            tl.cuenta(equity=float(v.value))

    ib.accountValueEvent += _cuenta_actualizada

    # -------------------------------------------------------------------------
    # SI COMPARTES CUENTA: tu propio equity, y de paso la EXPOSICION ABIERTA.
    #
    # Las dos cosas salen del mismo sitio, que es el estado que tu bot ya lleva:
    # que abriste, a que precio y cuanto llevas realizado. IBKR no te lo puede
    # dar: la posicion agregada no recuerda que clientId la abrio.
    #
    # `mis_posiciones` lo mantienes TU al abrir y al cerrar. Aqui va el esqueleto.
    # -------------------------------------------------------------------------
    CAPITAL_ASIGNADO = 10_000.0     # el trozo de cuenta de ESTE bot. Lo decides tu.
    # permId -> {"contrato":…, "cantidad":…, "precio":…, "multiplicador":…}
    # Lo llenas TU al abrir y lo vacias al cerrar. IBKR no puede decirte cuales son tuyas.
    mis_posiciones: dict[int, dict] = {}
    realizado_acumulado = 0.0

    def _flotante() -> float:
        """PnL no realizado de MIS posiciones. Cero abiertas -> 0.0, y es verdad."""
        total = 0.0
        for pos in mis_posiciones.values():
            tk = ib.ticker(pos["contrato"])
            precio = tk.marketPrice() if tk else None
            # Un precio que no ha llegado todavia sale NaN. Sumarlo envenena el
            # total entero con un NaN silencioso: se salta.
            if precio is None or precio != precio:
                continue
            total += (precio - pos["precio"]) * pos["cantidad"] * pos["multiplicador"]
        return total

    def _mi_equity() -> float:
        return equity_de_agente(CAPITAL_ASIGNADO, realizado_acumulado, _flotante())

    # Si compartes cuenta: pon UNICO_BOT_EN_ESTA_CUENTA = False y descomenta esto.
    #     tl.cuenta(equity=_mi_equity())   # dentro de tu bucle, no en un callback

    # Reconexion: NO es una muerte. Ver TRAMPA 3.
    #
    # Fijate en que la DESCONEXION no manda nada al panel. Es deliberado: el Gateway
    # se reinicia solo cada dia y vas a volver en segundos. Lo unico que se hace al
    # caerse es dejarlo por escrito en TU log; el panel se entera por el pulso, que
    # es quien sabe distinguir "no esta" de "no esta AHORA MISMO".
    ib.disconnectedEvent += lambda: print("[bot] se cayo la conexion con TWS; reconectando")
    ib.connectedEvent += tl.reconectado

    # LA COMISION LLEGA AQUI, no en el fill. Por eso el CLOSE se manda aqui.
    # OJO con el nombre del campo de comision: en ib_async 2.x es `report.commission`.
    # Las versiones nuevas de la TWS API renombraron ese campo a `commissionAndFees`;
    # si actualizas y esto empieza a mandar la comision en nulo, es eso. Compruebalo
    # con  print(report)  la primera vez, en vez de fiarte de este comentario.
    def _comision(trade, fill, report):
        tl.cerrar(
            direccion="BUY" if fill.execution.side == "BOT" else "SELL",
            resultado=report.realizedPNL,
            comision=report.commission,
            motivo="OTHER",              # ponlo tu: TP / SL / TRAIL / MANUAL...
            precio_ejecutado=fill.execution.price,
            perm_id=fill.execution.permId,
        )

    ib.commissionReportEvent += _comision

    try:
        while True:
            ib.sleep(1)
            # EL LATIDO LLEVA LA EXPOSICION ABIERTA (`flotante` / `abiertas`).
            #
            # Sin esto, la plataforma solo sabe de ti lo que has CERRADO — y un
            # sistema convexo (piramides, dejar correr al ganador) pasa meses
            # cerrando intentos fallidos pequenos mientras el pago entero vive en
            # la posicion que sigue abierta. Medido solo por cierres, el mejor
            # tramo de su vida se ve como una curva que baja.
            #
            # Se manda una vez al dia: es la foto diaria de lo que hay en el aire,
            # no un tick. Si no sabes el flotante, NO lo mandes: un cero dice
            # "medido y no hay nada abierto".
            tl.latido(
                senales=0,
                entradas=0,
                flotante=_flotante() if mis_posiciones else None,
                abiertas=len(mis_posiciones),
            )
    except KeyboardInterrupt:
        tl.detener("Ctrl+C")
        ib.disconnect()
