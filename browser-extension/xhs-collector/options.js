function sendMessage(message) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(message, (response) => resolve(response));
  });
}

async function load() {
  const state = await sendMessage({ type: 'XHS_GET_STATE' });
  if (!state?.ok) {
    document.getElementById('status').textContent = state?.error || '无法连接本机服务';
    return;
  }
  document.getElementById('baseUrl').value = state.settings.baseUrl || '';
  document.getElementById('commentLimit').value = state.settings.commentLimit ?? 0;
  document.getElementById('includeComments').checked = !!state.settings.includeComments;
}

document.getElementById('save').addEventListener('click', async () => {
  const settings = {
    baseUrl: document.getElementById('baseUrl').value.trim(),
    commentLimit: Number(document.getElementById('commentLimit').value || 0),
    includeComments: document.getElementById('includeComments').checked,
  };
  const response = await sendMessage({ type: 'XHS_SAVE_SETTINGS', settings });
  document.getElementById('status').textContent = response?.ok ? '已保存' : (response?.error || '保存失败');
});

load();
