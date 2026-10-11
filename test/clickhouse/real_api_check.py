#!/usr/bin/env python3
"""Runs clickhouse/jev_udf.py against the real API on a small labelled, synthetic dataset.

Reports accuracy, wall time, requests, input tokens and estimated cost. Nothing here comes from a database:
every text below was written for this check, so no customer data is sent.

The key is read from TYPESAFE_API_KEY in the repo's .env (or the environment) and is never printed.
Usage: python3 test/clickhouse/real_api_check.py [--api-url URL] [--model NAME]
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")
UDF = os.path.join(ROOT, "clickhouse", "jev_udf.py")

ANGRY_CONDITION = "the customer is angry or threatens to cancel"
ANGRY = [
    "This is the third time my order arrives broken. I want my money back today.",
    "I have been on hold for two hours. Cancel my subscription right now.",
    "Your support is useless, nobody answers and I am done with this company.",
    "Charged twice again! If this is not refunded I will dispute it with my bank.",
    "Absolutely unacceptable. I will be telling everyone to avoid your service.",
    "Estou furioso, o técnico não apareceu pela segunda vez. Quero cancelar o contrato.",
    "Isso é um absurdo, cobraram um valor que eu nunca autorizei. Vou procurar o Procon.",
    "Péssimo atendimento, ninguém resolve nada. Se não consertarem hoje eu cancelo.",
    "I am fed up with these outages. One more and I am moving to your competitor.",
    "Why do I keep paying for something that never works? Refund me and close my account.",
]
CALM = [
    "Thanks, the replacement arrived yesterday and everything works.",
    "Could you tell me how to change the email address on my account?",
    "Just confirming that the invoice for March was received. Have a nice week.",
    "The new dashboard looks great, the team is enjoying it.",
    "Is there a discount for paying annually instead of monthly?",
    "Obrigado pelo retorno rápido, o problema foi resolvido.",
    "Gostaria de saber o horário de funcionamento do suporte no feriado.",
    "Segue em anexo o comprovante solicitado. Fico à disposição.",
    "Please add my colleague to the account when you have a moment.",
    "The delivery was on time and the driver was very helpful.",
]

REASON_QUESTION = "Why was this workplace safety alert dismissed?"
REASON_OPTIONS = ["false alert", "authorized or justified deviation", "system test", "blank card",
                  "equipment removed briefly for a task", "person is not our worker"]
REASONS = [
    ("Falso alerta, não havia ninguém no local", "false alert"),
    ("Sombra confundida com uma pessoa", "false alert"),
    ("não era um alerta de verdade", "false alert"),
    ("The camera mistook a barrel for a person", "false alert"),
    ("Liberado pelo supervisor, riscos controlados e máquina parada", "authorized or justified deviation"),
    ("O cliente justificou o desvio por escrito", "authorized or justified deviation"),
    ("Atividade autorizada com permissão de trabalho emitida", "authorized or justified deviation"),
    ("Approved by the area manager, all hazards isolated", "authorized or justified deviation"),
    ("Teste do sistema feito pela equipe de implantação", "system test"),
    ("TESTE DE CAMERA", "system test"),
    ("Simulação para validar o alarme", "system test"),
    ("Commissioning test, not a real event", "system test"),
    ("O card veio sem conteúdo", "blank card"),
    ("cartão em branco, nada registrado", "blank card"),
    ("Cartão vazio, sem imagem", "blank card"),
    ("Empty card", "blank card"),
    ("Tirou a luva por um instante para assinar um documento", "equipment removed briefly for a task"),
    ("Funcionário removeu o capacete rapidamente para ajustar a jugular", "equipment removed briefly for a task"),
    ("Estava calçando as luvas naquele momento", "equipment removed briefly for a task"),
    ("Took off the goggles for a few seconds to wipe them", "equipment removed briefly for a task"),
    ("Não é funcionário da nossa empresa", "person is not our worker"),
    ("Visitante de outra contratada", "person is not our worker"),
    ("Pessoa de fora, não pertence ao nosso quadro", "person is not our worker"),
    ("That person works for a different contractor", "person is not our worker"),
]


def read_env_key():
    key = os.environ.get("TYPESAFE_API_KEY", "")
    path = os.path.join(ROOT, ".env")
    if not key and os.path.exists(path):
        with open(path) as f:
            for line in f:
                name, _, value = line.strip().partition("=")
                if name == "TYPESAFE_API_KEY":
                    key = value.strip().strip("'\"")
    return key


def run_udf(config_path, rows, key):
    env = dict(os.environ, TYPESAFE_API_KEY=key)
    stdin = "%d\n%s" % (len(rows), "".join(json.dumps(r) + "\n" for r in rows))
    t0 = time.time()
    proc = subprocess.run([sys.executable, UDF, config_path], input=stdin, env=env, capture_output=True, text=True,
                          timeout=300)
    elapsed = time.time() - t0
    if proc.returncode != 0:
        sys.exit("udf failed: " + proc.stderr.strip())
    return [json.loads(json.loads(line)["result"]) for line in proc.stdout.splitlines()], elapsed


def usage(config_path, key):
    proc = subprocess.run([sys.executable, UDF, "--stats", config_path], env=dict(os.environ, TYPESAFE_API_KEY=key),
                          capture_output=True, text=True, timeout=60)
    return json.loads(proc.stdout.splitlines()[-1])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="https://api.typesafe.ai/v1/systemone")
    parser.add_argument("--model", default="jev-latest")
    args = parser.parse_args()
    key = read_env_key()
    if not key and "typesafe.ai" in args.api_url:
        sys.exit("TYPESAFE_API_KEY is empty: put the key in %s" % os.path.join(ROOT, ".env"))

    with tempfile.TemporaryDirectory() as tmp:
        config_path = os.path.join(tmp, "jev.json")
        with open(config_path, "w") as f:
            json.dump({"api_url": args.api_url, "model": args.model, "cache_path": os.path.join(tmp, "cache.sqlite"),
                       "max_rows_per_day": 500}, f)

        noul_rows = [{"row": t, "question": ANGRY_CONDITION, "kind": "noul", "options": []} for t in ANGRY + CALM]
        expected = [True] * len(ANGRY) + [False] * len(CALM)
        answers, noul_s = run_udf(config_path, noul_rows, key)
        probs = [a["noul"] for a in answers]
        wrong = [(noul_rows[i]["row"], round(p, 3)) for i, p in enumerate(probs) if (p >= 0.5) != expected[i]]
        print("noul   %d/%d correct at 0.5 in %.2f s" % (len(probs) - len(wrong), len(probs), noul_s))
        print("       angry rows:  min p = %.3f   calm rows: max p = %.3f"
              % (min(probs[:len(ANGRY)]), max(probs[len(ANGRY):])))
        for text, p in wrong:
            print("       wrong (p=%s): %s" % (p, text))

        choice_rows = [{"row": t, "question": REASON_QUESTION, "kind": "choice", "options": REASON_OPTIONS}
                       for t, _ in REASONS]
        answers, choice_s = run_udf(config_path, choice_rows, key)
        wrong = [(t, label, a["choice"], a.get("confidence")) for (t, label), a in zip(REASONS, answers)
                 if a["choice"] != label]
        print("choice %d/%d correct in %.2f s" % (len(REASONS) - len(wrong), len(REASONS), choice_s))
        for text, label, got, confidence in wrong:
            print("       wrong: %r expected %r, got %r (confidence %s)" % (text, label, got, confidence))

        _, cached_s = run_udf(config_path, noul_rows + choice_rows, key)
        print("cache  second run of all %d rows in %.2f s" % (len(noul_rows) + len(choice_rows), cached_s))

        day = usage(config_path, key)
        print("usage  %d rows sent, %d requests, %d input tokens (%.0f per row), estimated $%.6f at list price"
              % (day["rows_sent"], day["requests"], day["input_tokens"],
                 day["input_tokens"] / max(day["rows_sent"], 1), day["estimated_cost_usd"]))


if __name__ == "__main__":
    main()
