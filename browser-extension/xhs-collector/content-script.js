(() => {
  if (window.__XHS_COLLECTOR_INSTALLED__) {
    return;
  }
  window.__XHS_COLLECTOR_INSTALLED__ = true;

  const STYLE_ID = 'xhs-collector-style';
  const PANEL_ID = 'xhs-collector-float';

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) {
      return;
    }
    const style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = `
      #${PANEL_ID} {
        position: fixed;
        right: 18px;
        bottom: 18px;
        z-index: 2147483647;
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 10px 12px;
        border-radius: 999px;
        background: linear-gradient(135deg, rgba(17,24,39,.96), rgba(15,118,110,.92));
        color: #fff;
        box-shadow: 0 18px 50px rgba(0,0,0,.28);
        font: 600 12px/1.2 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif;
      }
      #${PANEL_ID} button {
        border: 0;
        border-radius: 999px;
        padding: 8px 12px;
        background: rgba(255,255,255,.14);
        color: #fff;
        cursor: pointer;
      }
      #${PANEL_ID} button:hover {
        background: rgba(255,255,255,.24);
      }
      .xhs-collector-toast {
        position: fixed;
        left: 50%;
        bottom: 28px;
        transform: translateX(-50%);
        z-index: 2147483647;
        padding: 10px 14px;
        border-radius: 999px;
        background: rgba(17,24,39,.96);
        color: #fff;
        font: 500 12px/1.2 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif;
        box-shadow: 0 16px 44px rgba(0,0,0,.22);
      }
    `;
    document.head.appendChild(style);
  }

  function toast(message) {
    const node = document.createElement('div');
    node.className = 'xhs-collector-toast';
    node.textContent = message;
    document.body.appendChild(node);
    setTimeout(() => {
      node.remove();
    }, 2600);
  }

  async function collectCurrentPage() {
    const title = document.title || '';
    const url = location.href;
    toast('正在采集本页...');
    chrome.runtime.sendMessage(
      {
        type: 'XHS_COLLECT_PAGE',
        payload: {
          url,
          title,
          include_comments: true,
          comment_limit: 0,
        },
      },
      (response) => {
        if (!response || !response.ok) {
          toast(`采集失败：${response?.error || '未知错误'}`);
          return;
        }
        const result = response.result || {};
        toast(`采集完成：帖子 ${result.post_count || 0}，评论 ${result.comment_count || 0}`);
      },
    );
  }

  function mountPanel() {
    if (document.getElementById(PANEL_ID)) {
      return;
    }
    const wrap = document.createElement('div');
    wrap.id = PANEL_ID;
    wrap.innerHTML = `
      <span>小红书采集器</span>
      <button type="button">采集本页</button>
    `;
    wrap.querySelector('button').addEventListener('click', collectCurrentPage);
    document.body.appendChild(wrap);
  }

  ensureStyles();
  chrome.runtime.onMessage.addListener((message) => {
    if (message?.type === 'XHS_COLLECTED') {
      const result = message.result || {};
      toast(`采集完成：帖子 ${result.post_count || 0}，评论 ${result.comment_count || 0}`);
    }
    if (message?.type === 'XHS_COLLECT_FAILED') {
      toast(`采集失败：${message.error || '未知错误'}`);
    }
  });
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountPanel, { once: true });
  } else {
    mountPanel();
  }
})();
