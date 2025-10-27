export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const { pathname, searchParams } = url;

      // 上傳圖片到R2
      if (request.method === "POST" && pathname === "/upload") {
        // 1) 先驗證 Token（避免洩漏內部細節）
        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 2) 再檢查必要 binding
        const badEnv = Validators.env(env, "DIVINATION_BUCKET", j);
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

        // 5) 基本欄位檢查 + key 驗證（允許中文）
        if (!fileBase64 || !key) {
          return j({ ok: false, error: "missing_fileBase64_or_key" }, 400);
        }

        // 嘗試從 URL 編碼還原；做一致化
        try {
          key = decodeURIComponent(key);
        } catch (_) {}
        key = key.normalize("NFC");

        // 安全檢查（允許 Unicode）
        const byteLen = new TextEncoder().encode(key).length;
        if (!key || byteLen > 1024) {
          return j({ ok: false, error: "bad_key_length" }, 400);
        }
        if (key.startsWith("/") || key.endsWith("/") || key.includes("..")) {
          return j({ ok: false, error: "bad_key_path", key }, 400);
        }
        if (/[\u0000-\u001F\u007F]/.test(key)) {
          return j({ ok: false, error: "bad_key_control_chars", key }, 400);
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
            const head = await env.DIVINATION_BUCKET.head(key);
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
          putRes = await env.DIVINATION_BUCKET.put(key, bytes, {
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

      // 測試 worker 是否正常
      if (request.method === "GET" && pathname === "/healthz") {
        return j({ ok: true }, 200);
      }

      // 更新快取：POST /updateCacheCardId 將所有抓到的牌ID 丟到R@
      if (request.method === "POST" && pathname === "/updateCacheCardId") {
        // 1) 驗證 Token（先做，避免洩漏內部細節）
        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 2) 檢查必要 binding
        const badEnv = Validators.env(env, "DIVINATION_BUCKET", j);
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
            await env.DIVINATION_BUCKET.put(key, payload, {
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

      // 隨機取卡：GET /getCardId?deck=love&n=1 可選擇數量 這是不包含每日限制 測試用的
      if (request.method === "GET" && pathname === "/getCardId") {
        // 0) 必要 binding
        const badEnv = Validators.env(env, "DIVINATION_BUCKET", j);
        if (badEnv) return badEnv;

        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 1) 讀取 query
        const deckRaw = (searchParams.get("deck") || "").toString().trim();
        if (!deckRaw)
          return dailyResp({
            ok: false,
            message: "missing_deck",
            error: "missing_deck",
          });

        const res = await getRandomIdsFromDeck(
          env,
          deckRaw,
          searchParams.get("n")
        );

        if (res.error) {
          const isNotFound =
            res.error === "cache_not_found" || res.error === "cache_empty";
          return dailyResp({
            ok: false,
            message: isNotFound ? "deck_not_found_or_empty" : "deck_error",
            deck: res.deck,
            ids: [],
            totalInDeck: res.totalInDeck,
            updatedAt: res.updatedAt,
            error: res.error,
          });
        }

        // 成功取得牌組
        return dailyResp({
          ok: true,
          message: "deck_draw_success",
          deck: res.deck,
          ids: res.ids,
          totalInDeck: res.totalInDeck,
          updatedAt: res.updatedAt,
        });
      }

      // 檢查今日使用者是否已經抽過牌
      if (request.method === "POST" && pathname === "/checkDaily") {
        // 1) 驗證 Token（先做，避免洩漏內部細節）
        const badAuth = Validators.auth(request, env, j);
        if (badAuth) return badAuth;

        // 2) 檢查必要 binding
        const badEnv = Validators.env(env, "DIVINATION_BUCKET", j);
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

        // 5) 取出userId 並組合成key 檢查是否有重複 格式為 id + 日期
        const userId = String(body.userId || "").trim();
        const quota = await checkAndMarkDailyR2(env, userId);
        // 今日已使用
        if (quota.used) {
          return dailyResp({
            used: true,
            date: quota.date,
            message: "today_already_used",
          });
        }

        // 同時回一張牌，避免 n8n 再打一趟（防止競態）
        // 傳入的deck分類
        const deck = body.deck || null;
        const n = body.n;

        if (deck) {
          const res = await getRandomIdsFromDeck(env, deck, n);
          if (res.error) {
            return dailyResp({
              ok: false,
              used: false,
              date: quota.date,
              message: "today_not_used_but_deck_error",
              deck,
              ids: [],
              totalInDeck: res.totalInDeck ?? 0,
              updatedAt: res.updatedAt ?? null,
              error: res.error,
            });
          }
          return dailyResp({
            used: false,
            date: quota.date,
            message: "today_marked_and_cards_ready",
            deck: res.deck,
            ids: res.ids,
            totalInDeck: res.totalInDeck,
            updatedAt: res.updatedAt,
          });
        }

        // 沒帶 deck 就只回今日未使用
        return dailyResp({
          used: false,
          date: quota.date,
          message: "today_not_used",
        });
      }

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

function dailyResp({
  ok = true,
  used,
  date,
  message,
  deck = null,
  ids = [],
  totalInDeck = null,
  updatedAt = null,
  error = null,
}) {
  return j(
    { ok, used, date, message, deck, ids, totalInDeck, updatedAt, error },
    ok ? 200 : 400
  );
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

// 從陣列中取 k 個不重複元素
function sampleUnique(arr, n) {
  const len = arr.length;
  const k = Math.max(0, Math.min(n | 0, len));
  if (k === 0) return [];
  if (k === len) return shuffleThenSlice(arr, len);

  // 部分 Fisher–Yates：只洗出前 k 個
  const a = arr.slice();
  for (let i = 0; i < k; i++) {
    const j = i + ((Math.random() * (len - i)) | 0);
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, k);
}

const Validators = {
  env(env, key, j) {
    if (!env || !env[key]) {
      return j(
        {
          ok: false,
          error: `missing_${String(key).toLowerCase()}`,
          hint: `Check your binding name; use env.${String(key)}`,
        },
        500
      );
    }
  },

  contentType(ct, j) {
    const v = String(ct || "").toLowerCase(); // 允許帶 ; charset=utf-8
    if (!v.includes("application/json")) {
      return j(
        {
          ok: false,
          error: "content_type_must_be_application_json",
          got: ct,
        },
        400
      );
    }
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

// 取得「台灣時區」的 YYYYMMDD 作為當日Key
function taipeiDateKey() {
  // sv-SE -> 2025-10-08 12:34:56
  const s = new Date().toLocaleString("sv-SE", { timeZone: "Asia/Taipei" });
  return s.slice(0, 10).replace(/-/g, ""); // -> 20251008
}

async function checkAndMarkDailyR2(env, userId) {
  const date = taipeiDateKey();
  const key = `quota/draw-${date}/${encodeURIComponent(userId)}.json`;

  // 1) 先HEAD：就代表今天用過了
  const head = await env.DIVINATION_BUCKET.head(key);
  if (head) return { used: true, date };

  // 2) 嘗試寫入一個很小的佔位物件
  //    （並非嚴格原子：極端併發下仍可能雙寫）
  try {
    await env.DIVINATION_BUCKET.put(
      key,
      JSON.stringify({ userId, at: new Date().toISOString() }),
      {
        httpMetadata: {
          contentType: "application/json",
          cacheControl: "no-store",
        },
        // 也可加上短 TTL 的清理流程，或由排程刪舊日資料
      }
    );
  } catch (_) {
    // 少數情況下並發衝突會在這裡補捉
    return { used: true, date };
  }

  return { used: false, date };
}

// 與寫入端一致的白名單（可共用常數）
const ALLOWED = new Set(["love", "money", "career", "daily"]);

// 清洗傳入的Deck文字
function normDeck(raw) {
  const d = String(raw || "")
    .trim()
    .replace(/[\/\\]/g, "-");
  if (!ALLOWED.has(d)) throw new Error("invalid_deck");
  return d;
}

// 1.env 2.分類名 3.回傳 抽到牌實例的數量
async function getRandomIdsFromDeck(env, deckRaw, nRaw) {
  const MAX_N = 1;
  const deck = normDeck(deckRaw);
  const nReq = parseInt(nRaw ?? "1", 10);
  const n = Number.isFinite(nReq) && nReq > 0 ? Math.min(nReq, MAX_N) : 1;

  let obj;
  const file = await env.DIVINATION_BUCKET.get(`cache/card-ids-${deck}.json`);
  if (!file)
    return {
      deck,
      ids: [],
      totalInDeck: 0,
      updatedAt: null,
      error: "cache_not_found",
    };

  try {
    obj = JSON.parse(await file.text());
  } catch {
    return {
      deck,
      ids: [],
      totalInDeck: 0,
      updatedAt: null,
      error: "bad_cache_json",
    };
  }

  const pool = Array.isArray(obj?.ids)
    ? obj.ids.map(String).filter(Boolean)
    : [];
  if (!pool.length)
    return {
      deck,
      ids: [],
      totalInDeck: 0,
      updatedAt: obj?.updatedAt ?? null,
      error: "cache_empty",
    };

  const count = Math.min(n, pool.length);
  const ids =
    count === pool.length
      ? shuffleThenSlice(pool, count)
      : sampleUnique(pool, count);

  return {
    deck,
    ids,
    totalInDeck: pool.length,
    updatedAt: obj?.updatedAt ?? null,
    error: null,
  };
}
