const DEFAULT_SETTINGS = {
  baseUrl: 'http://127.0.0.1:8787',
  commentLimit: 0,
  includeComments: true,
  sourcePlatform: '小红书',
};

const HISTORY_KEY = 'xhs_recent_tasks';
const SETTINGS_KEY = 'xhs_plugin_settings';
const LAST_RESULT_KEY = 'xhs_last_result';
const HISTORY_LIMIT = 10;
const MENU_COLLECT = 'xhs_collect_page';
const MENU_REFRESH = 'xhs_refresh_page';

function storageGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

function normalizeSettings(partial = {}) {
  return {
    ...DEFAULT_SETTINGS,
    ...partial,
    baseUrl: String(partial.baseUrl || DEFAULT_SETTINGS.baseUrl).trim().replace(/\/$/, ''),
    commentLimit: Number.isFinite(Number(partial.commentLimit)) ? Number(partial.commentLimit) : DEFAULT_SETTINGS.commentLimit,
    includeComments: partial.includeComments !== false,
    sourcePlatform: String(partial.sourcePlatform || DEFAULT_SETTINGS.sourcePlatform).trim() || DEFAULT_SETTINGS.sourcePlatform,
  };
}

async function getSettings() {
  const data = await storageGet([SETTINGS_KEY]);
  return normalizeSettings(data[SETTINGS_KEY] || {});
}

async function saveSettings(nextSettings) {
  const settings = normalizeSettings(nextSettings);
  await storageSet({ [SETTINGS_KEY]: settings });
  return settings;
}

async function appendHistory(entry) {
  const data = await storageGet([HISTORY_KEY]);
  const history = Array.isArray(data[HISTORY_KEY]) ? data[HISTORY_KEY] : [];
  history.unshift(entry);
  await storageSet({ [HISTORY_KEY]: history.slice(0, HISTORY_LIMIT) });
}

async function setLastResult(result) {
  await storageSet({ [LAST_RESULT_KEY]: result });
}

async function getLastResult() {
  const data = await storageGet([LAST_RESULT_KEY]);
  return data[LAST_RESULT_KEY] || null;
}

async function requestJson(baseUrl, path, method = 'GET', body) {
  const url = new URL(path, baseUrl).toString();
  const response = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`服务返回了非 JSON: ${text.slice(0, 200)}`);
  }
  if (!response.ok || data.error) {
    throw new Error(data.error || `请求失败: ${response.status}`);
  }
  return data;
}

async function collectTask(payload, mode = 'collect') {
  const settings = await getSettings();
  const baseUrl = settings.baseUrl;
  const body = {
    url: payload.url,
    title: payload.title || '',
    comment_limit: Number.isFinite(Number(payload.comment_limit)) ? Number(payload.comment_limit) : settings.commentLimit,
    include_comments: payload.include_comments !== undefined ? !!payload.include_comments : settings.includeComments,
    company_name: payload.company_name || '',
    source_platform: payload.source_platform || settings.sourcePlatform,
  };
  const path = mode === 'refresh' ? '/api/xhs-plugin/refresh' : '/api/xhs-plugin/collect';
  const result = await requestJson(baseUrl, path, 'POST', body);
  await setLastResult(result);
  await appendHistory({
    id: result.artifact_id || `task-${Date.now()}`,
    url: body.url,
    title: body.title || '',
    status: 'success',
    recordCount: result.record_count || 0,
    commentCount: result.comment_count || 0,
    postCount: result.post_count || 0,
    createdAt: new Date().toISOString(),
    downloadCsvUrl: result.download_csv_url || '',
    downloadJsonUrl: result.download_json_url || '',
  });
  return result;
}

async function downloadLastResult(format) {
  const lastResult = await getLastResult();
  if (!lastResult) {
    throw new Error('暂无最近结果');
  }
  const url = format === 'json' ? lastResult.download_json_url : lastResult.download_csv_url;
  if (!url) {
    throw new Error('结果文件不存在');
  }
  const filename = format === 'json' ? 'xiaohongshu-result.json' : 'xiaohongshu-result.csv';
  await new Promise((resolve, reject) => {
    chrome.downloads.download(
      {
        url,
        filename,
        saveAs: true,
      },
      (downloadId) => {
        const error = chrome.runtime.lastError;
        if (error) {
          reject(new Error(error.message));
          return;
        }
        resolve(downloadId);
      },
    );
  });
  return { ok: true, filename, url };
}

async function setBadge(text, color = '#0f766e') {
  try {
    await chrome.action.setBadgeBackgroundColor({ color });
    await chrome.action.setBadgeText({ text });
  } catch {
    // Best effort only.
  }
}

async function clearBadge() {
  try {
    await chrome.action.setBadgeText({ text: '' });
  } catch {
    // Best effort only.
  }
}

function isXiaohongshuUrl(url = '') {
  return /^https:\/\/www\.xiaohongshu\.com\//i.test(url);
}

function sendTabMessage(tabId, message) {
  return new Promise((resolve) => {
    try {
      chrome.tabs.sendMessage(tabId, message, () => {
        resolve();
      });
    } catch {
      resolve();
    }
  });
}

chrome.runtime.onInstalled.addListener(async () => {
  const data = await storageGet([SETTINGS_KEY, HISTORY_KEY]);
  if (!data[SETTINGS_KEY]) {
    await saveSettings(DEFAULT_SETTINGS);
  }
  if (!Array.isArray(data[HISTORY_KEY])) {
    await storageSet({ [HISTORY_KEY]: [] });
  }
  chrome.contextMenus.removeAll(() => {
    chrome.contextMenus.create({
      id: MENU_COLLECT,
      title: '采集本页帖子 + 评论',
      contexts: ['page'],
      documentUrlPatterns: ['https://www.xiaohongshu.com/*'],
    });
    chrome.contextMenus.create({
      id: MENU_REFRESH,
      title: '刷新并采集帖子 + 评论',
      contexts: ['page'],
      documentUrlPatterns: ['https://www.xiaohongshu.com/*'],
    });
  });
});

chrome.contextMenus.onClicked.addListener((info, tab) => {
  (async () => {
    if (!tab?.url || !isXiaohongshuUrl(tab.url)) {
      throw new Error('请在小红书页面上使用');
    }
    await setBadge('采集中');
    const payload = {
      url: tab.url,
      title: tab.title || '',
      comment_limit: 0,
      include_comments: true,
    };
    const mode = info.menuItemId === MENU_REFRESH ? 'refresh' : 'collect';
    const result = await collectTask(payload, mode);
    await setBadge('OK', '#0f766e');
    setTimeout(() => clearBadge(), 2500);
    if (tab.id) {
      await sendTabMessage(tab.id, {
        type: 'XHS_COLLECTED',
        result,
        mode,
      });
    }
  })().catch(async (error) => {
    await setBadge('ERR', '#b91c1c');
    setTimeout(() => clearBadge(), 3500);
    if (tab?.id) {
      await sendTabMessage(tab.id, {
        type: 'XHS_COLLECT_FAILED',
        error: error.message || String(error),
      });
    }
  });
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const type = message?.type;
  (async () => {
    if (type === 'XHS_GET_STATE') {
      const settings = await getSettings();
      const data = await storageGet([HISTORY_KEY, LAST_RESULT_KEY]);
      let health = { ok: false, service: { online: false }, error: '服务不可用' };
      try {
        health = await requestJson(settings.baseUrl, '/api/xhs-plugin/status');
      } catch (error) {
        health = {
          ok: false,
          service: { online: false },
          error: error.message || String(error),
        };
      }
      sendResponse({
        ok: true,
        settings,
        health,
        history: Array.isArray(data[HISTORY_KEY]) ? data[HISTORY_KEY] : [],
        lastResult: data[LAST_RESULT_KEY] || null,
      });
      return;
    }
    if (type === 'XHS_SAVE_SETTINGS') {
      const settings = await saveSettings(message.settings || {});
      sendResponse({ ok: true, settings });
      return;
    }
    if (type === 'XHS_COLLECT_PAGE') {
      const result = await collectTask(message.payload || {}, 'collect');
      sendResponse({ ok: true, result });
      return;
    }
    if (type === 'XHS_REFRESH_PAGE') {
      const result = await collectTask(message.payload || {}, 'refresh');
      sendResponse({ ok: true, result });
      return;
    }
    if (type === 'XHS_DOWNLOAD_LAST') {
      const result = await downloadLastResult(message.format || 'csv');
      sendResponse({ ok: true, result });
      return;
    }
    if (type === 'XHS_GET_HISTORY') {
      const data = await storageGet([HISTORY_KEY]);
      sendResponse({ ok: true, history: Array.isArray(data[HISTORY_KEY]) ? data[HISTORY_KEY] : [] });
      return;
    }
    sendResponse({ ok: false, error: 'Unknown message type' });
  })().catch((error) => {
    sendResponse({ ok: false, error: error.message || String(error) });
  });
  return true;
});
