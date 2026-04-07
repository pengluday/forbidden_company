function sendMessage(message) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(message, (response) => {
      resolve(response);
    });
  });
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function setValue(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.value = value;
  }
}

function setChecked(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.checked = !!value;
  }
}

async function loadState() {
  const response = await sendMessage({ type: 'XHS_GET_STATE' });
  if (!response || !response.ok) {
    setText('serviceBadge', '离线');
    setText('resultStatus', response?.error || '服务不可用');
    return;
  }

  const { settings, health, history, lastResult } = response;
  setValue('baseUrl', settings.baseUrl);
  setValue('commentLimit', settings.commentLimit);
  setChecked('includeComments', settings.includeComments);
  setText('serviceBadge', health?.service?.online ? '在线' : '离线');
  const tab = await getActiveTab();
  setText('pageTitle', tab?.title || lastResult?.preview_rows?.[0]?.source_title || '等待采集');
  setText('pageUrl', tab?.url || lastResult?.preview_rows?.[0]?.source_url || '-');
  setText('postCount', String(lastResult?.post_count || 0));
  setText('commentCount', String(lastResult?.comment_count || 0));
  setText('recordCount', String(lastResult?.record_count || 0));
  setText('resultStatus', lastResult ? '上次采集已完成' : '等待采集');

  const historyList = document.getElementById('historyList');
  historyList.innerHTML = '';
  (history || []).slice(0, 5).forEach((item) => {
    const row = document.createElement('div');
    row.className = 'history-item';
    row.innerHTML = `
      <strong>${escapeHtml(item.title || item.url || '未命名任务')}</strong>
      <div class="muted">${escapeHtml(item.status || 'unknown')} · 帖子 ${item.postCount || 0} · 评论 ${item.commentCount || 0}</div>
    `;
    historyList.appendChild(row);
  });
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab || null;
}

async function collectCurrentPage(mode = 'collect') {
  const tab = await getActiveTab();
  if (!tab?.url) {
    setText('resultStatus', '未找到当前页面');
    return;
  }
  const payload = {
    url: tab.url,
    title: tab.title || '',
    comment_limit: Number(document.getElementById('commentLimit').value || 0),
    include_comments: document.getElementById('includeComments').checked,
  };
  setText('resultStatus', '采集中...');
  const type = mode === 'refresh' ? 'XHS_REFRESH_PAGE' : 'XHS_COLLECT_PAGE';
  const response = await sendMessage({ type, payload });
  if (!response || !response.ok) {
    setText('resultStatus', response?.error || '采集失败');
    return;
  }
  const result = response.result || {};
  setText('postCount', String(result.post_count || 0));
  setText('commentCount', String(result.comment_count || 0));
  setText('recordCount', String(result.record_count || 0));
  setText('pageTitle', tab.title || result.preview_rows?.[0]?.source_title || '采集完成');
  setText('pageUrl', tab.url);
  setText('resultStatus', `采集完成：帖子 ${result.post_count || 0}，评论 ${result.comment_count || 0}`);
  await loadState();
}

async function saveSettings() {
  const settings = {
    baseUrl: document.getElementById('baseUrl').value.trim(),
    commentLimit: Number(document.getElementById('commentLimit').value || 0),
    includeComments: document.getElementById('includeComments').checked,
  };
  const response = await sendMessage({ type: 'XHS_SAVE_SETTINGS', settings });
  if (response?.ok) {
    setText('serviceBadge', '已保存');
    await loadState();
  }
}

async function downloadLast(format) {
  const response = await sendMessage({ type: 'XHS_DOWNLOAD_LAST', format });
  if (!response?.ok) {
    setText('resultStatus', response?.error || '无法下载');
  }
}

document.getElementById('saveSettings').addEventListener('click', saveSettings);
document.getElementById('collectPage').addEventListener('click', () => collectCurrentPage('collect'));
document.getElementById('refreshState').addEventListener('click', loadState);
document.getElementById('downloadCsv').addEventListener('click', () => downloadLast('csv'));
document.getElementById('downloadJson').addEventListener('click', () => downloadLast('json'));

loadState();
