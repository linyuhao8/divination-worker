export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const { pathname, searchParams } = url;

      if (request.method === "POST" && pathname === "/upload") {
        // 1) 先驗證 Token（避免洩漏內部細節）
        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 2) 再檢查必要 binding
        const badEnv = Validators.env(env, "R2_BUCKET", j);
        if (badEnv) return badEnv;

        // 3) Content-Type 檢查
        const badCt = Validators.contentType(
          request.headers.get("content-type") || "",
          j
        );
        if (badCt) return badCt;

        // 4) 解析 JSON
        const body = await Validators.json(request, j);
        if (body?.ok === false) return body;

        let { fileBase64, key, overwrite, contentType } = body || {};
        key = (key || "").toString().trim();

        // 5) 基本欄位檢查
        if (!fileBase64 || !key) {
          return j({ ok: false, error: "missing_fileBase64_or_key" }, 400);
        }
        if (
          !/^[A-Za-z0-9._\-\/]+$/.test(key) ||
          key.includes("..") ||
          key.startsWith("/") ||
          key.endsWith("/")
        ) {
          return j({ ok: false, error: "bad_key_format", key }, 400);
        }

        const allowOverwrite =
          String(overwrite ?? "false").toLowerCase() === "true";
        if (
          typeof contentType !== "string" ||
          !/^[\w.+-]+\/[\w.+-]+$/.test(contentType)
        ) {
          contentType = "application/octet-stream";
        }

        // 6) base64 大小保護（可調）
        const MAX_BYTES = 10 * 1024 * 1024;
        const estBytes = Math.floor((fileBase64.length * 3) / 4);
        if (estBytes > MAX_BYTES)
          return j({ ok: false, error: "payload_too_large" }, 413);

        // 7) 解 base64
        let bytes;
        try {
          bytes = decodeBase64Flexible(fileBase64);
        } catch (e) {
          return j({ ok: false, error: "bad_base64", detail: String(e) }, 400);
        }

        // 8) 覆蓋檢查（若可用，建議改成條件式 put 以避免競態）
        if (!allowOverwrite) {
          try {
            const head = await env.R2_BUCKET.head(key);
            if (head)
              return j({ ok: false, error: "object_already_exists", key }, 409);
          } catch (e) {
            return j(
              { ok: false, error: "r2_head_error", detail: String(e) },
              500
            );
          }
        }

        // 9) 寫入 R2
        let putRes;
        try {
          putRes = await env.R2_BUCKET.put(key, bytes, {
            httpMetadata: {
              contentType,
              cacheControl: "public, max-age=31536000, immutable",
            },
            customMetadata: { via: "json-base64" },
            // 若支援條件寫入，這裡可以加（示意）：
            // onlyIf: { /* doesNotExist: true 或 ifNoneMatch: '*' 等 */ }
          });
        } catch (e) {
          return j(
            { ok: false, error: "r2_put_error", detail: String(e) },
            500
          );
        }

        return j(
          {
            ok: true,
            key,
            size: bytes.byteLength,
            etag: putRes?.etag || null,
            // location: publicUrlFor(key) // 若有的話
          },
          200
        );
      }

      if (request.method === "GET" && pathname === "/healthz") {
        return j({ ok: true }, 200);
      }

      // 🟢 更新快取：POST /updateCacheCardId
      if (request.method === "POST" && pathname === "/updateCacheCardId") {
        // 1) 驗證 Token（先做，避免洩漏內部細節）
        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 2) 檢查必要 binding
        const badEnv = Validators.env(env, "R2_BUCKET", j);
        if (badEnv) return badEnv;

        // 3) Content-Type 必須是 JSON
        const badCt = Validators.contentType(
          request.headers.get("content-type") || "",
          j
        );
        if (badCt) return badCt;

        // 4) 解析 JSON
        const body = await Validators.json(request, j);
        if (body?.ok === false) return body;

        // 支援：[{deck, ids}, ...] 或 {deck, ids}
        const entries = Array.isArray(body) ? body : [body];

        // 空 payload 防呆
        if (entries.length === 0 || (entries.length === 1 && !entries[0])) {
          return j({ ok: false, error: "empty_payload" }, 400);
        }

        // 安全與資源保護（可依需求調整）
        const MAX_DECKS_PER_REQ = 20;
        const MAX_IDS_PER_DECK = 5000;
        const MAX_PAYLOAD_BYTES = 512 * 1024; // 512KB
        const estimatedBytes = Math.floor(
          (JSON.stringify(body).length * 3) / 4
        );
        if (estimatedBytes > MAX_PAYLOAD_BYTES) {
          return j({ ok: false, error: "payload_too_large" }, 413);
        }
        if (entries.length > MAX_DECKS_PER_REQ) {
          return j(
            { ok: false, error: "too_many_decks", limit: MAX_DECKS_PER_REQ },
            400
          );
        }

        // 可接受的牌庫
        const ALLOWED = new Set(["love", "money", "career", "daily"]);

        const saved = [];
        const errors = [];

        // 用 entries.entries() 拿到 index 與內容
        for (const [index, raw] of entries.entries()) {
          const e = raw || {};
          const deck = String(e.deck || "").trim();

          if (!ALLOWED.has(deck)) {
            errors.push({ deck, reason: "invalid_deck", index });
            continue;
          }

          // 正規化 ids：轉字串、trim、過濾空字串、去重
          const ids = Array.isArray(e.ids)
            ? Array.from(
                new Set(
                  e.ids.map((v) => String(v || "").trim()).filter(Boolean)
                )
              )
            : [];

          if (ids.length === 0) {
            errors.push({ deck, reason: "empty_ids_array", index });
            continue;
          }
          if (ids.length > MAX_IDS_PER_DECK) {
            errors.push({
              deck,
              reason: "too_many_ids",
              limit: MAX_IDS_PER_DECK,
              index,
            });
            continue;
          }

          const payloadObj = {
            ids,
            total: ids.length,
            updatedAt: new Date().toISOString(),
          };
          const payload = JSON.stringify(payloadObj);

          // 每 deck 的 payload 大小限制（額外保護）
          if (payload.length > MAX_PAYLOAD_BYTES) {
            errors.push({ deck, reason: "deck_payload_too_large", index });
            continue;
          }

          const key = `cache/card-ids-${deck}.json`;

          try {
            await env.R2_BUCKET.put(key, payload, {
              httpMetadata: {
                contentType: "application/json",
                cacheControl: "no-store",
              },
              customMetadata: { via: "batch-update" },
            });
            saved.push({ deck, count: ids.length, key });
          } catch (err) {
            errors.push({
              deck,
              reason: "r2_put_error",
              detail: String(err),
              index,
            });
          }
        }

        const status = errors.length ? 207 /* Multi-Status */ : 200;
        return j({ ok: errors.length === 0, saved, errors }, status);
      }

      // 🟡 隨機取卡：GET /getCardId?deck=love&n=1
      if (request.method === "GET" && pathname === "/getCardId") {
        // 0) 必要 binding
        const badEnv = Validators.env(env, "R2_BUCKET", j);
        if (badEnv) return badEnv;

        // 1) 讀取 query
        const deckRaw = (searchParams.get("deck") || "").toString().trim();
        if (!deckRaw) return j({ ok: false, error: "missing_deck" }, 400);

        // 與寫入端一致的白名單（可共用常數）
        const ALLOWED = new Set(["love", "money", "career", "daily"]);
        // 檔名安全處理（仍保留，避免奇怪字元）
        const deck = deckRaw.replace(/[\/\\]/g, "-");
        if (!ALLOWED.has(deck)) {
          return j({ ok: false, error: "invalid_deck", deck }, 400);
        }

        // 2) 參數 n：最小 1、最大上限（避免濫用）
        const MAX_N = 50;
        const nReq = parseInt(searchParams.get("n") || "1", 10);
        const n = Number.isFinite(nReq) && nReq > 0 ? Math.min(nReq, MAX_N) : 1;

        // 3) 讀取快取
        let obj;
        try {
          const file = await env.R2_BUCKET.get(`cache/card-ids-${deck}.json`);
          if (!file) {
            return j({ ok: false, error: "cache_not_found", deck }, 404);
          }
          obj = JSON.parse(await file.text());
        } catch (e) {
          return j(
            { ok: false, error: "r2_get_error", deck, detail: String(e) },
            500
          );
        }

        // 4) 檢查 pool
        const pool = Array.isArray(obj?.ids)
          ? obj.ids.map(String).filter(Boolean)
          : [];
        if (pool.length === 0) {
          return j({ ok: false, error: "cache_empty", deck }, 404);
        }

        // 5) 隨機取 n 個不重複（若 n > pool 長度，就取 pool 長度）
        const count = Math.min(n, pool.length);
        const ids =
          count === pool.length
            ? shuffleThenSlice(pool, count) // 取全部時用洗牌更快
            : sampleUnique(pool, count);

        return j(
          {
            ok: true,
            deck,
            ids,
            totalInDeck: pool.length,
            updatedAt: obj?.updatedAt ?? null,
          },
          200
        );
      }

      return j({ ok: false, error: "not_found" }, 404);
    } catch (e) {
      // 所有未預期錯誤
      return j(
        { ok: false, error: "unhandled_exception", detail: String(e) },
        500
      );
    }
  },
};

// 設定200回應
function j(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

// 更健壯的 base64 轉位元組：
// - 去掉 data: 前綴
// - 修正 URL-safe base64（- _ → + /）
// - 移除空白／換行
// - 補齊 padding (=)
function decodeBase64Flexible(input) {
  let s = String(input || "");

  // data URL 前綴
  const i = s.indexOf(",");
  if (s.startsWith("data:") && i !== -1) {
    s = s.slice(i + 1);
  }
  // 去空白
  s = s.replace(/\s+/g, "");
  // URL-safe -> 標準
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  // 補 padding
  const pad = s.length % 4;
  if (pad === 2) s += "==";
  else if (pad === 3) s += "=";
  else if (pad === 1) throw new Error("invalid_base64_length");

  // 解碼
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// 小工具：洗牌後切片（Fisher–Yates）
function shuffleThenSlice(arr, k) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, k);
}

const Validators = {
  env(env, key, j) {
    return validate(
      !!env[key],
      {
        error: `missing_${String(key).toLowerCase()}`,
        hint: `Check your binding name; use env.${String(key)}`,
        code: 500,
      },
      j
    );
  },

  contentType(ct, j) {
    return validate(
      (ct || "").toLowerCase().includes("application/json"),
      {
        error: "content_type_must_be_application_json",
        got: ct,
        code: 400,
      },
      j
    );
  },
  auth(request, env, j) {
    const auth = request.headers.get("authorization") || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : null;
    if (token !== env.UPLOAD_TOKEN) {
      return j(
        {
          ok: false,
          error: "unauthorized",
          hint: "Missing or invalid Bearer token",
        },
        401
      );
    }
  },
  async json(request, j) {
    try {
      return request.json();
    } catch (e) {
      return j(
        {
          ok: false,
          error: "invalid_json",
          detail: String(e),
        },
        400
      );
    }
  },
};

// --- 共用 Helper ---
function validate(condition, { error, hint, got, code = 400 }, j) {
  if (!condition) {
    return j(
      { ok: false, error, ...(hint && { hint }), ...(got && { got }) },
      code
    );
  }
}
