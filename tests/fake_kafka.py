"""Doble en memoria de los clientes de confluent-kafka usados por la capa Bronze."""
from __future__ import annotations

import zlib
from collections import defaultdict


class MensajeFake:
    def __init__(self, topic, partition, offset, key, value, headers, ts_ms):
        self._t, self._p, self._o, self._k, self._v, self._h, self._ts = topic, partition, offset, key, value, headers, ts_ms

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def headers(self): return self._h
    def timestamp(self): return (1, self._ts)
    def error(self): return None


class BrokerFake:
    def __init__(self, particiones: int = 3):
        self.n = particiones
        self.topics: dict[str, list[list[MensajeFake]]] = defaultdict(lambda: [[] for _ in range(particiones)])

    def total(self, topic: str) -> int:
        return sum(len(p) for p in self.topics[topic])


class ProductorFake:
    def __init__(self, broker: BrokerFake, fallar_en_flush: int | None = None):
        self.b, self.fallar_en_flush, self.flushes, self._pend = broker, fallar_en_flush, 0, []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        p = zlib.crc32(key or b"") % self.b.n
        lista = self.b.topics[topic][p]
        msg = MensajeFake(topic, p, len(lista), key, value, headers, 1_780_000_000_000 + len(lista))
        lista.append(msg)
        self._pend.append((on_delivery, msg))

    def poll(self, timeout=0): return 0

    def flush(self, timeout=None):
        self.flushes += 1
        if self.fallar_en_flush is not None and self.flushes == self.fallar_en_flush:
            raise RuntimeError("caída simulada del productor durante flush")
        for cb, msg in self._pend:
            if cb:
                cb(None, msg)
        self._pend = []
        return 0


class ConsumidorFake:
    def __init__(self, broker: BrokerFake, fallar_tras: int | None = None):
        self.b, self.fallar_tras, self.entregados = broker, fallar_tras, 0
        self.pos: dict[int, int] = {}
        self.topic = None
        self.commits = []
        self._rr = 0

    def assign(self, tps):
        self.topic = tps[0].topic
        self.pos = {tp.partition: max(tp.offset, 0) for tp in tps}

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (0, len(self.b.topics[tp.topic][tp.partition]))

    def poll(self, timeout=None):
        if self.fallar_tras is not None and self.entregados >= self.fallar_tras:
            raise RuntimeError("caída simulada del consumidor")
        parts = sorted(self.pos)
        for k in range(len(parts)):
            p = parts[(self._rr + k) % len(parts)]
            lista = self.b.topics[self.topic][p]
            if self.pos[p] < len(lista):
                msg = lista[self.pos[p]]
                self.pos[p] += 1
                self._rr = (self._rr + k + 1) % len(parts)
                self.entregados += 1
                return msg
        return None

    def commit(self, offsets=None, asynchronous=False): self.commits.append(offsets)
    def close(self): pass
