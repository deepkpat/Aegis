import random
import sys
import uuid

import requests
from locust import FastHttpUser, between, events, task

CONFIG = {
    "host": "http://localhost:8000",
    "num_keys": 10000,
    "key_prefix": "locust",
    "seed_on_start": True,
    "miss_pct": 5.0,
    "payload_bytes": 256,
    "hotspot_pct": 80.0,
    "hotspot_n": 500,
    "read_pct": 80.0,
    "patch_pct": 12.0,
    "put_pct": 2.0,
    "create_pct": 5.0,
    "delete_pct": 0.0,
    "min_wait_s": 0.01,
    "max_wait_s": 0.1,
}


def key_for(i):
    return f"{CONFIG['key_prefix']}-{i:08d}"


def make_payload(extra=None):
    body = {"data": "x" * max(0, CONFIG["payload_bytes"])}
    if extra is not None:
        body["seq"] = extra
    return {"id": None, "payload": body}


def pick_key():
    if random.random() * 100 < CONFIG["miss_pct"]:
        return f"{CONFIG['key_prefix']}-missing-{uuid.uuid4().hex[:12]}", False
    if random.random() * 100 < CONFIG["hotspot_pct"]:
        upper = min(CONFIG["hotspot_n"], CONFIG["num_keys"])
        return key_for(random.randrange(upper)), True
    return key_for(random.randrange(CONFIG["num_keys"])), True


def seed(host, num=None):
    num = CONFIG["num_keys"] if num is None else num
    s = requests.Session()
    created, existed, failed = 0, 0, 0
    filler = "x" * max(0, CONFIG["payload_bytes"])
    for i in range(num):
        kid = f"{CONFIG['key_prefix']}-{i:08d}"
        try:
            r = s.post(
                f"{host}/v1/records",
                json={"id": kid, "payload": {"data": filler, "seed": True}},
                timeout=30,
            )
        except requests.RequestException:
            failed += 1
            continue
        if r.status_code == 201:
            created += 1
        elif r.status_code == 409:
            existed += 1
        else:
            failed += 1
        if (i + 1) % 1000 == 0:
            print(f"seed {i + 1}/{num} created={created} existed={existed} failed={failed}")
    print(f"seed done total={num} created={created} existed={existed} failed={failed}")
    return created, existed, failed


@events.test_start.add_listener
def _on_test_start(environment, **kwargs):
    if not CONFIG["seed_on_start"]:
        return
    seed(environment.host or CONFIG["host"])


class ApiUser(FastHttpUser):
    wait_time = between(CONFIG["min_wait_s"], CONFIG["max_wait_s"])

    def on_start(self):
        self.versions = {}

    @task
    def workload(self):
        roll = random.random() * 100.0
        read = CONFIG["read_pct"]
        patch = read + CONFIG["patch_pct"]
        put = patch + CONFIG["put_pct"]
        create = put + CONFIG["create_pct"]
        delete = create + CONFIG["delete_pct"]
        if roll < read:
            self.do_get()
        elif roll < patch:
            self.do_patch()
        elif roll < put:
            self.do_put()
        elif roll < create:
            self.do_create()
        elif roll < delete:
            self.do_delete()
        else:
            self.do_health()

    def do_get(self):
        kid, _ = pick_key()
        with self.client.get(f"/v1/records/{kid}", catch_response=True) as r:
            if r.status_code == 200:
                try:
                    self.versions[kid] = r.json()["version"]
                except (ValueError, KeyError):
                    pass
                r.success()
            elif r.status_code == 404:
                r.success()
            else:
                r.failure(f"GET {r.status_code}: {(r.text or "")[:200]}")

    def do_patch(self):
        kid, _ = pick_key()
        version = self.versions.get(kid)
        if version is None:
            g = self.client.get(f"/v1/records/{kid}")
            if g.status_code == 200:
                try:
                    version = g.json()["version"]
                    self.versions[kid] = version
                except (ValueError, KeyError):
                    return
            else:
                return
        payload = {"data": "x" * max(0, CONFIG["payload_bytes"]), "upd": uuid.uuid4().hex[:8]}
        with self.client.patch(
            f"/v1/records/{kid}",
            json={"payload": payload, "expected_version": version},
            catch_response=True,
        ) as r:
            if r.status_code == 200:
                try:
                    self.versions[kid] = r.json()["version"]
                except (ValueError, KeyError):
                    pass
                r.success()
            elif r.status_code in (404, 412):
                self.versions.pop(kid, None)
                r.success()
            else:
                r.failure(f"PATCH {r.status_code}: {(r.text or "")[:200]}")

    def do_put(self):
        kid, _ = pick_key()
        payload = {"data": "x" * max(0, CONFIG["payload_bytes"]), "upd": uuid.uuid4().hex[:8]}
        with self.client.put(
            f"/v1/records/{kid}",
            json={"payload": payload},
            catch_response=True,
        ) as r:
            if r.status_code in (200, 201):
                try:
                    self.versions[kid] = r.json()["version"]
                except (ValueError, KeyError):
                    pass
                r.success()
            elif r.status_code == 400:
                r.success()
            else:
                r.failure(f"PUT {r.status_code}: {(r.text or '')[:200]}")

    def do_create(self):
        kid = f"{CONFIG['key_prefix']}-new-{uuid.uuid4().hex[:12]}"
        body = make_payload(extra=random.randrange(1 << 30))
        body["id"] = kid
        with self.client.post("/v1/records", json=body, catch_response=True) as r:
            if r.status_code in (201, 400, 409):
                r.success()
            else:
                r.failure(f"POST {r.status_code}: {(r.text or "")[:200]}")

    def do_delete(self):
        kid, _ = pick_key()
        with self.client.delete(f"/v1/records/{kid}", catch_response=True) as r:
            if r.status_code in (204, 404):
                r.success()
            else:
                r.failure(f"DELETE {r.status_code}: {(r.text or "")[:200]}")
                return
        self.versions.pop(kid, None)
        if r.status_code == 204:
            filler = "x" * max(0, CONFIG["payload_bytes"])
            self.client.post(
                "/v1/records",
                json={"id": kid, "payload": {"data": filler, "healed": True}},
            )

    def do_health(self):
        if random.random() < 0.5:
            self.client.get("/health")
        else:
            self.client.get("/ready")


if __name__ == "__main__":
    seed(sys.argv[1] if len(sys.argv) > 1 else CONFIG["host"])
