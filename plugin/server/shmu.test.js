"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const grib2 = require("./grib2");
const shmu = require("./shmu");

test("station pressure is reduced to mean sea level", () => {
  const msl = shmu.reducePressureToMsl(980, 200, 10);
  assert.ok(msl > 980);
  assert.equal(shmu.reducePressureToMsl(1013.2, 0, 15), 1013.2);
  assert.equal(shmu.reducePressureToMsl(null, 100, 10), null);
});

test("simple-packed ALADIN hour decodes a plausible Bratislava temperature", async () => {
  const url =
    "https://opendata.shmu.sk/meteorology/weather/nwp/aladin/sk/4.5km/20261112/1800/al-grib_sk_012-20261112-1800-nwp-.grb";
  const dest = path.join(os.tmpdir(), "shmu-test-aladin-012.grb");
  if (!fs.existsSync(dest) || fs.statSync(dest).size < 1000) {
    const res = await fetch(url, { headers: { "User-Agent": "open-meteo-weather/0.1.0 test" } });
    assert.equal(res.ok, true);
    fs.writeFileSync(dest, Buffer.from(await res.arrayBuffer()));
  }
  const messages = grib2.parseMessages(fs.readFileSync(dest));
  assert.ok(messages.length > 5);
  const grid = grib2.parseGrid(messages[0][3]);
  assert.equal(grid.template, 33);
  assert.ok(grid.nx > 10 && grid.ny > 10);
  const ij = grib2.latLonToIj(48.15, 17.11, grid);
  let tempK = null;
  for (const msg of messages) {
    if (!msg[4] || !msg[5] || !msg[6] || !msg[7]) continue;
    const product = grib2.parseProduct(msg[4]);
    if (product.category !== 0 || product.number !== 0 || product.surfaceType !== 103) continue;
    if (product.height !== 2) continue;
    const unpacked = grib2.unpackSimple(msg[5], msg[6], msg[7]);
    tempK = grib2.sampleBilinear(unpacked.values, unpacked.bitmap, grid, ij.i, ij.j);
    break;
  }
  assert.ok(tempK != null, "2 m temperature field missing");
  const celsius = tempK - 273.15;
  assert.ok(celsius > -40 && celsius < 45, `unexpected temperature ${celsius}`);
});
