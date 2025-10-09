export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const { pathname, searchParams } = url;

      if (request.method === "POST" && pathname === "/upload") {
        // 0) 檢查 binding
        if (!env.R2_BUCKET) {
          return j(
            {
              ok: false,
              error: "missing_r2_binding",
              hint: "Check your binding name; use env.<YOUR_BINDING>",
            },
            500
          );
        }

        // 1) 要 JSON
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json")) {
          return j(
            {
              ok: false,
              error: "content_type_must_be_application_json",
              got: ct,
            },
            400
          );
        }

        // 2) 解析 JSON
        let body;
        try {
          body = await request.json();
        } catch (e) {
          return j(
            { ok: false, error: "invalid_json", detail: String(e) },
            400
          );
        }

        let { fileBase64, key, overwrite, contentType } = body || {};
        key = (key || "").toString().trim();
        const allowOverwrite =
          String(overwrite ?? "false").toLowerCase() === "true";
        contentType =
          typeof contentType === "string" && contentType
            ? contentType
            : "application/octet-stream";

        if (!fileBase64 || !key) {
          return j({ ok: false, error: "missing_fileBase64_or_key" }, 400);
        }

        // 3) 修正／解碼 base64
        let bytes;
        try {
          bytes = decodeBase64Flexible(fileBase64);
        } catch (e) {
          return j({ ok: false, error: "bad_base64", detail: String(e) }, 400);
        }

        // 4) 覆蓋檢查
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

        // 5) 寫入 R2
        let putRes;
        try {
          putRes = await env.R2_BUCKET.put(key, bytes, {
            httpMetadata: {
              contentType,
              cacheControl: "public, max-age=31536000, immutable",
            },
            customMetadata: { via: "json-base64" },
          });
        } catch (e) {
          return j(
            { ok: false, error: "r2_put_error", detail: String(e) },
            500
          );
        }

        return j(
          { ok: true, key, size: bytes.byteLength, etag: putRes?.etag || null },
          200
        );
      }

      if (request.method === "GET" && pathname === "/healthz") {
        return new Response("ok", { status: 200 });
      }

      // 🟢 更新快取：POST /updateCacheCardId
      if (request.method === "POST" && pathname === "/updateCacheCardId") {
        // 1) 驗證 token（可沿用你既有的）
        const auth = request.headers.get("authorization") || "";
        if (
          !auth.startsWith("Bearer ") ||
          auth.slice(7).trim() !== env.UPLOAD_TOKEN
        ) {
          return new Response(
            JSON.stringify({ ok: false, error: "unauthorized" }),
            {
              status: 401,
              headers: { "content-type": "application/json" },
            }
          );
        }

        // 2) 要 JSON
        const ct = request.headers.get("content-type") || "";
        if (!ct.includes("application/json")) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: "content_type_must_be_application_json",
              got: ct,
            }),
            {
              status: 400,
              headers: { "content-type": "application/json" },
            }
          );
        }

        // 3) 解析＋正規化為多筆 entries
        let body;
        try {
          body = await request.json();
        } catch (e) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: "invalid_json",
              detail: String(e),
            }),
            {
              status: 400,
              headers: { "content-type": "application/json" },
            }
          );
        }

        // 支援：[{deck, ids}, ...] 或 {deck, ids}
        const entries = Array.isArray(body) ? body : [body];

        // 可接受的牌庫
        const ALLOWED = new Set(["love", "money", "career", "daily"]);

        const saved = [];
        const errors = [];

        for (const idx in entries) {
          const e = entries[idx] || {};
          const deck = String(e.deck || "").trim();
          const ids = Array.isArray(e.ids)
            ? e.ids.map(String).filter(Boolean)
            : [];

          if (!ALLOWED.has(deck)) {
            errors.push({ deck, reason: "invalid_deck", index: Number(idx) });
            continue;
          }
          if (ids.length === 0) {
            errors.push({
              deck,
              reason: "empty_ids_array",
              index: Number(idx),
            });
            continue;
          }

          const payload = JSON.stringify({
            ids,
            total: ids.length,
            updatedAt: new Date().toISOString(),
          });

          const key = `cache/card-ids-${deck}.json`;
          try {
            await env.R2_BUCKET.put(key, payload, {
              httpMetadata: {
                contentType: "application/json",
                cacheControl: "no-store",
              },
            });
            saved.push({ deck, count: ids.length, key });
          } catch (err) {
            errors.push({
              deck,
              reason: "r2_put_error",
              detail: String(err),
              index: Number(idx),
            });
          }
        }

        const status = errors.length ? 207 /* Multi-Status */ : 200;
        return new Response(
          JSON.stringify({ ok: errors.length === 0, saved, errors }, null, 2),
          {
            status,
            headers: { "content-type": "application/json" },
          }
        );
      }

      // 🟡 隨機取卡：GET /getCardId?deck=love&n=1
      if (request.method === "GET" && pathname === "/getCardId") {
        if (!env.R2_BUCKET) {
          return new Response(
            JSON.stringify({ ok: false, error: "missing_r2_binding" }),
            {
              status: 500,
              headers: { "content-type": "application/json" },
            }
          );
        }

        const deckRaw = (searchParams.get("deck") || "").toString().trim();
        if (!deckRaw) {
          return new Response(
            JSON.stringify({ ok: false, error: "missing_deck" }),
            {
              status: 400,
              headers: { "content-type": "application/json" },
            }
          );
        }
        // 檔名安全處理（避免出現 / 或 \）
        const deck = deckRaw.replace(/[\/\\]/g, "-");

        let obj;
        try {
          const file = await env.R2_BUCKET.get(`cache/card-ids-${deck}.json`);
          if (!file) {
            return new Response(
              JSON.stringify({ ok: false, error: "cache_not_found", deck }),
              {
                status: 404,
                headers: { "content-type": "application/json" },
              }
            );
          }
          obj = JSON.parse(await file.text());
        } catch (e) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: "r2_get_error",
              deck,
              detail: String(e),
            }),
            {
              status: 500,
              headers: { "content-type": "application/json" },
            }
          );
        }

        const pool = Array.isArray(obj?.ids)
          ? obj.ids.map(String).filter(Boolean)
          : [];
        if (pool.length === 0) {
          return new Response(
            JSON.stringify({ ok: false, error: "cache_empty", deck }),
            {
              status: 404,
              headers: { "content-type": "application/json" },
            }
          );
        }

        const nReq = parseInt(searchParams.get("n") || "1", 10);
        const n =
          Number.isFinite(nReq) && nReq > 0 ? Math.min(nReq, pool.length) : 1;

        // 取 n 個不重複
        const pick = sampleUnique(pool, n);

        return new Response(
          JSON.stringify({
            ok: true,
            deck,
            ids: pick,
            totalInDeck: pool.length,
            updatedAt: obj?.updatedAt ?? null,
          }),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      }

      return new Response("not found", { status: 404 });
    } catch (e) {
      // 所有未預期錯誤
      return j(
        { ok: false, error: "unhandled_exception", detail: String(e) },
        500
      );
    }
  },
};

// 設定回應
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

// ====== 新增的小工具函式 ======
function sampleUnique(arr, k) {
  // 洗牌取前 k 個（Fisher-Yates）
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, k);
}
