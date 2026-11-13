"use strict";

function u32(buf, i) {
  return buf.readUInt32BE(i);
}

function signMagnitude(buf, i, bytes) {
  let v = 0;
  for (let k = 0; k < bytes; k++) v = (v << 8) | buf[i + k];
  const sign = 1 << (bytes * 8 - 1);
  if (v & sign) return -(v & (sign - 1));
  return v;
}

function readBits(data, bitPos, nbits) {
  let v = 0;
  for (let b = 0; b < nbits; b++) {
    const byte = data[(bitPos + b) >> 3];
    if (byte === undefined) return null;
    v = (v << 1) | ((byte >> (7 - ((bitPos + b) & 7))) & 1);
  }
  return v;
}

function bitmapHas(bitmap, index) {
  if (!bitmap) return true;
  const byte = bitmap[index >> 3];
  if (byte === undefined) return false;
  return ((byte >> (7 - (index & 7))) & 1) === 1;
}

function packedIndex(bitmap, gridIndex) {
  if (!bitmap) return gridIndex;
  if (!bitmapHas(bitmap, gridIndex)) return null;
  let n = 0;
  // Count set bits before gridIndex. Bitmaps are small (4512 bits).
  const fullBytes = gridIndex >> 3;
  for (let i = 0; i < fullBytes; i++) {
    n += popcount(bitmap[i]);
  }
  const rem = gridIndex & 7;
  if (rem) {
    const byte = bitmap[fullBytes] ?? 0;
    for (let b = 0; b < rem; b++) {
      if ((byte >> (7 - b)) & 1) n += 1;
    }
  }
  return n;
}

function popcount(v) {
  v = v - ((v >> 1) & 0x55);
  v = (v & 0x33) + ((v >> 2) & 0x33);
  return (((v + (v >> 4)) & 0x0f) * 0x01) & 0xff;
}

function parseGrid(section3) {
  const oct = (n, width = 4, signed = false) => {
    const i = n - 1;
    return signed ? signMagnitude(section3, i, width) : section3.readUIntBE(i, width);
  };
  const template = section3.readUInt16BE(12);
  if (template !== 33 && template !== 30) {
    throw new Error(`Unsupported GRIB2 grid template ${template}`);
  }
  return {
    template,
    nPoints: u32(section3, 6),
    shape: section3[14],
    nx: oct(31),
    ny: oct(35),
    la1: oct(39, 4, true) / 1e6,
    lo1: oct(43) / 1e6,
    flags: section3[46],
    lad: oct(48, 4, true) / 1e6,
    lov: oct(52) / 1e6,
    dx: oct(56) / 1000,
    dy: oct(60) / 1000,
    projectionCentre: section3[63],
    scan: section3[64],
    latin1: oct(66, 4, true) / 1e6,
    latin2: oct(70, 4, true) / 1e6,
  };
}

function lambertXy(lat, lon, grid, radius = 6371229) {
  const toRad = (d) => (d * Math.PI) / 180;
  const latr = toRad(lat);
  const lonr = toRad(lon);
  const ladr = toRad(grid.lad);
  const lovr = toRad(grid.lov);
  const l1 = toRad(grid.latin1);
  const l2 = toRad(grid.latin2);
  const n =
    Math.abs(l1 - l2) < 1e-12
      ? Math.sin(l1)
      : Math.log(Math.cos(l1) / Math.cos(l2)) /
        Math.log(Math.tan(Math.PI / 4 + l2 / 2) / Math.tan(Math.PI / 4 + l1 / 2));
  const F = (Math.cos(l1) * Math.pow(Math.tan(Math.PI / 4 + l1 / 2), n)) / n;
  const rho = (radius * F) / Math.pow(Math.tan(Math.PI / 4 + latr / 2), n);
  const rho0 = (radius * F) / Math.pow(Math.tan(Math.PI / 4 + ladr / 2), n);
  const theta = n * (lonr - lovr);
  return { x: rho * Math.sin(theta), y: rho0 - rho * Math.cos(theta) };
}

function latLonToIj(lat, lon, grid) {
  const p = lambertXy(lat, lon, grid);
  const o = lambertXy(grid.la1, grid.lo1, grid);
  return { i: (p.x - o.x) / grid.dx, j: (p.y - o.y) / grid.dy };
}

function unpackSimple(section5, section6, section7) {
  const count = u32(section5, 5);
  const R = section5.readFloatBE(11);
  const E = signMagnitude(section5, 15, 2);
  const D = signMagnitude(section5, 17, 2);
  const nbits = section5[19];
  const bitmap = section6[5] === 0 ? section6.subarray(6) : null;
  const data = section7.subarray(5);
  const scaleB = Math.pow(2, E);
  const scaleD = Math.pow(10, -D);
  const values = new Array(count);
  let bitPos = 0;
  const totalBits = data.length * 8;
  for (let k = 0; k < count; k++) {
    if (nbits === 0) {
      values[k] = R * scaleD;
      continue;
    }
    if (bitPos + nbits > totalBits) {
      values[k] = null;
      continue;
    }
    const raw = readBits(data, bitPos, nbits);
    bitPos += nbits;
    values[k] = (R + raw * scaleB) * scaleD;
  }
  return { values, bitmap, count };
}

function inDomain(i, j, grid, bitmap) {
  if (i < 0 || j < 0 || i > grid.nx - 1 || j > grid.ny - 1) return false;
  const ii = Math.round(i);
  const jj = Math.round(j);
  if (ii < 0 || jj < 0 || ii >= grid.nx || jj >= grid.ny) return false;
  const idx = jj * grid.nx + ii;
  return bitmapHas(bitmap, idx);
}

function sampleBilinear(values, bitmap, grid, i, j) {
  if (!inDomain(i, j, grid, bitmap)) return null;
  const i0 = Math.floor(i);
  const j0 = Math.floor(j);
  const i1 = i0 + 1;
  const j1 = j0 + 1;
  const corners = [
    [i0, j0, (i1 - i) * (j1 - j)],
    [i1, j0, (i - i0) * (j1 - j)],
    [i0, j1, (i1 - i) * (j - j0)],
    [i1, j1, (i - i0) * (j - j0)],
  ];
  let num = 0;
  let den = 0;
  let ok = true;
  for (const [ii, jj, w] of corners) {
    if (w <= 0) continue;
    if (ii < 0 || jj < 0 || ii >= grid.nx || jj >= grid.ny) {
      ok = false;
      break;
    }
    const p = packedIndex(bitmap, jj * grid.nx + ii);
    if (p == null || values[p] == null) {
      ok = false;
      break;
    }
    num += values[p] * w;
    den += w;
  }
  if (ok && den > 0) return num / den;
  const ii = Math.round(i);
  const jj = Math.round(j);
  const p = packedIndex(bitmap, jj * grid.nx + ii);
  return p == null ? null : values[p];
}

function parseProduct(section4) {
  const template = section4.readUInt16BE(7);
  const category = section4[9];
  const number = section4[10];
  const timeUnit = section4[17];
  const forecastTime = signMagnitude(section4, 18, 4);
  const surfaceType = section4[22];
  const surfaceScale = signMagnitude(section4, 23, 1);
  const surfaceValue = signMagnitude(section4, 24, 4);
  const height =
    surfaceScale === 0 ? surfaceValue : surfaceValue * Math.pow(10, -surfaceScale);
  return { template, category, number, timeUnit, forecastTime, surfaceType, height };
}

function parseMessages(buf) {
  const out = [];
  let i = 0;
  while (i < buf.length) {
    const j = buf.indexOf("GRIB", i, "ascii");
    if (j < 0) break;
    if (buf[j + 7] !== 2) {
      i = j + 4;
      continue;
    }
    const msglen = Number(buf.readBigUInt64BE(j + 8));
    const end = j + msglen;
    const discipline = buf[j + 6];
    const sections = { discipline };
    let pos = j + 16;
    while (pos + 5 <= end && buf.toString("ascii", pos, pos + 4) !== "7777") {
      const seclen = u32(buf, pos);
      const secnum = buf[pos + 4];
      if (seclen < 5 || pos + seclen > end) break;
      sections[secnum] = buf.subarray(pos, pos + seclen);
      pos += seclen;
    }
    out.push(sections);
    i = end;
  }
  return out;
}

module.exports = {
  parseGrid,
  parseProduct,
  parseMessages,
  unpackSimple,
  latLonToIj,
  inDomain,
  sampleBilinear,
};
