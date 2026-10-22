#!/usr/bin/env python3
import csv, math, os, statistics, threading, time
from pathlib import Path
import zmq

HOST, PORT = "tcp://127.0.0.1", 5567
REPETITIONS, MESSAGES = 3, 300
SCENARIOS = {"baja": 256, "media": 4096, "alta": 16384}

def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered)-1, max(0, math.ceil(p/100*len(ordered))-1))]

def server(stop, ready, stats):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(f"{HOST}:{PORT}")
    ready.set()
    count = total = 0
    while not stop.is_set():
        if not sock.poll(100, zmq.POLLIN):
            continue
        try:
            data = sock.recv()
            count += 1
            total += len(data)
            sock.send(data)
        except zmq.ZMQError:
            break
    stats.update(received=count, bytes_received=total)
    sock.close()
    ctx.term()

def main():
    stop, ready, stats = threading.Event(), threading.Event(), {}
    thread = threading.Thread(target=server, args=(stop, ready, stats), daemon=True)
    thread.start()
    if not ready.wait(5):
        raise RuntimeError("El servidor ZMQ no inició.")
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    sock.setsockopt(zmq.SNDTIMEO, 5000)
    sock.connect(f"{HOST}:{PORT}")
    rows, summaries = [], []
    try:
        time.sleep(0.2)
        for scenario, size in SCENARIOS.items():
            payload = os.urandom(size)
            for rep in range(1, REPETITIONS + 1):
                rtts, received_bytes = [], 0
                start = time.perf_counter()
                for msg_id in range(1, MESSAGES + 1):
                    t0 = time.perf_counter_ns()
                    sock.send(payload)
                    reply = sock.recv()
                    rtt = (time.perf_counter_ns() - t0) / 1e6
                    rtts.append(rtt)
                    received_bytes += len(reply)
                    rows.append({"escenario": scenario, "repeticion": rep,
                        "mensaje_id": msg_id, "bytes_enviados": size,
                        "bytes_recibidos": len(reply), "latencia_rtt_ms": f"{rtt:.6f}"})
                duration = time.perf_counter() - start
                summaries.append({"escenario": scenario, "repeticion": rep,
                    "tamano_mensaje_bytes": size, "duracion_s": f"{duration:.6f}",
                    "enviados": MESSAGES, "recibidos": len(rtts),
                    "bytes_utiles_recibidos": received_bytes,
                    "throughput_MiB_s": f"{received_bytes/1048576/duration:.6f}",
                    "latencia_rtt_media_ms": f"{statistics.mean(rtts):.6f}",
                    "latencia_rtt_mediana_ms": f"{statistics.median(rtts):.6f}",
                    "latencia_rtt_p95_ms": f"{percentile(rtts,95):.6f}",
                    "latencia_rtt_min_ms": f"{min(rtts):.6f}",
                    "latencia_rtt_max_ms": f"{max(rtts):.6f}",
                    "jitter_stddev_ms": f"{statistics.pstdev(rtts):.6f}",
                    "mensajes_perdidos_o_sin_respuesta": MESSAGES-len(rtts)})
                print(f"{scenario} rep {rep}: RTT media={statistics.mean(rtts):.3f} ms; "
                      f"P95={percentile(rtts,95):.3f} ms; "
                      f"throughput={received_bytes/1048576/duration:.3f} MiB/s")
    finally:
        sock.close()
        ctx.term()
        stop.set()
        thread.join(timeout=2)
    for name, data in (("zmq_mediciones.csv", rows), ("zmq_resumen.csv", summaries)):
        with Path(name).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    print("\nArchivos creados: zmq_mediciones.csv y zmq_resumen.csv")
    print("Esto mide RTT de ZMQ/TCP en localhost, no la latencia de la red 5G completa.")
    print("Mensajes recibidos por servidor:", stats.get("received", 0))

if __name__ == "__main__":
    main()
