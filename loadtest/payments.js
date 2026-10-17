// Нагрузка на приём платежей: постоянный поток POST /payments с уникальными ключами
// идемпотентности, 10% запросов — повтор уже использованного ключа.
//
//   docker compose --profile loadtest run --rm k6 run /scripts/payments.js
//   RATE=500 DURATION=60s ... — уровень нагрузки (запросов в секунду) и длительность.
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://api:8000";
const API_KEY = __ENV.API_KEY || "change-me";
const RATE = Number(__ENV.RATE || 200);
// Метка прогона в ключе: второй прогон не должен попадать в ключи первого.
const RUN = Date.now().toString(36);

const rateLimited = new Counter("rate_limited");

export const options = {
  scenarios: {
    accept: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Math.max(50, RATE),
      maxVUs: RATE * 4,
    },
  },
  summaryTrendStats: ["avg", "med", "p(95)", "p(99)", "max"],
  thresholds: {
    // 429 сверх лимита — ожидаемый ответ (setResponseCallback ниже), в долю сбоев не входит.
    http_req_failed: ["rate<0.01"],
  },
};

http.setResponseCallback(http.expectedStatuses(202, 429));

export default function () {
  const replay = __ITER > 0 && Math.random() < 0.1;
  // Ключ уникален в пределах прогона: VU и номер итерации; повтор берёт ключ прошлой итерации.
  const key = `lt-${RUN}-${__VU}-${replay ? __ITER - 1 : __ITER}`;
  const body = JSON.stringify({
    amount: "100.00",
    currency: "RUB",
    description: "load test",
    webhook_url: "http://webhook-sink:9000/hook",
  });
  const res = http.post(`${BASE_URL}/api/v1/payments`, body, {
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": API_KEY,
      "Idempotency-Key": key,
    },
    tags: { name: "POST /payments" },
  });
  if (res.status === 429) rateLimited.add(1);
  check(res, { "202 or 429": (r) => r.status === 202 || r.status === 429 });
}
