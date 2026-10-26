const PAGE_SIZE = 10;
const REFRESH_INTERVAL = 10_000;
const ORDER_FILTER_DEBOUNCE = 250;

const state = {
  view: 'overview',
  eventsPage: 0,
  ordersPage: 0,
  dltPage: 0,
  eventsStatus: '',
  ordersStatus: '',
  refreshTimer: null,
  orderDebounceTimer: null,
  toastTimer: null,
  // Per-endpoint tokens so stale responses can't overwrite newer ones
  tokens: { dashboard: 0, events: 0, orders: 0, dlt: 0 }
};

const elements = {
  views: [...document.querySelectorAll('.view')],
  navItems: [...document.querySelectorAll('.nav-item')],
  breadcrumb: document.querySelector('#view-breadcrumb'),
  updated: document.querySelector('#updated-label'),
  refresh: document.querySelector('#refresh-button'),
  connectionDot: document.querySelector('#connection-dot'),
  connectionLabel: document.querySelector('#connection-label'),
  connectionDetail: document.querySelector('#connection-detail'),
  apiState: document.querySelector('#api-state'),
  kafkaState: document.querySelector('#kafka-state'),
  dbState: document.querySelector('#db-state'),
  healthPulse: document.querySelector('#health-pulse'),
  recentEvents: document.querySelector('#recent-events-body'),
  eventBody: document.querySelector('#events-body'),
  orderBody: document.querySelector('#orders-body'),
  dltBody: document.querySelector('#dlt-body'),
  eventFilter: document.querySelector('#events-filter'),
  orderFilter: document.querySelector('#orders-filter'),
  publishForm: document.querySelector('#publish-form'),
  publishSubmit: document.querySelector('#publish-submit'),
  publishFeedback: document.querySelector('#publish-feedback'),
  jsonPreview: document.querySelector('#json-preview'),
  toast: document.querySelector('#toast'),
  navDltCount: document.querySelector('#nav-dlt-count'),
  metrics: {
    events: document.querySelector('#metric-events'),
    orders: document.querySelector('#metric-orders'),
    pending: document.querySelector('#metric-pending'),
    dead: document.querySelector('#metric-dead')
  }
};

const pageConfig = {
  events: {
    body: elements.eventBody,
    label: '#events-page-label',
    previous: '#events-prev',
    next: '#events-next',
    cols: 6,
    noun: 'events',
    endpoint: '/api/events'
  },
  orders: {
    body: elements.orderBody,
    label: '#orders-page-label',
    previous: '#orders-prev',
    next: '#orders-next',
    cols: 4,
    noun: 'orders',
    endpoint: '/api/orders'
  },
  dlt: {
    body: elements.dltBody,
    label: '#dlt-page-label',
    previous: '#dlt-prev',
    next: '#dlt-next',
    cols: 5,
    noun: 'dead-letter events',
    endpoint: '/api/dead-letter-events'
  }
};

const viewTitles = {
  overview: 'Overview',
  publish: 'Publish event',
  events: 'Event stream',
  orders: 'Orders',
  'dead-letters': 'Dead letters'
};

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  })[c]);
}

function shortId(value) {
  const id = String(value ?? '');
  if (!id) return '—';
  return id.length > 20 ? `${id.slice(0, 9)}…${id.slice(-7)}` : id;
}

function statusBadge(status) {
  const value = String(status || 'UNKNOWN').toUpperCase();
  const className = value.toLowerCase().replaceAll('_', '-');
  return `<span class="status-badge status-${escapeHtml(className)}">${escapeHtml(value.replaceAll('_', ' '))}</span>`;
}

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return escapeHtml(value);
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  }).format(date);
}

function setLoading(target, colspan) {
  target.innerHTML = `<tr><td colspan="${colspan}" class="empty-cell">Loading…</td></tr>`;
}

function setEmpty(target, colspan, message) {
  target.innerHTML = `<tr><td colspan="${colspan}" class="empty-cell">${escapeHtml(message)}</td></tr>`;
}

function showToast(message, isError = false) {
  clearTimeout(state.toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.toggle('error', isError);
  elements.toast.classList.add('show');
  state.toastTimer = setTimeout(() => elements.toast.classList.remove('show'), 4200);
}

async function apiRequest(path, options = {}) {
  let response;
  try {
    response = await fetch(path, {
      ...options,
      headers: {
        Accept: 'application/json',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...options.headers
      }
    });
  } catch {
    throw new Error('Could not reach the backend. Check that the Spring Boot application is running.');
  }

  const body = await response.text();

  if (!response.ok) {
    let detail = body;
    try {
      const parsed = JSON.parse(body);
      detail = parsed.detail || parsed.message || parsed.error || body;
    } catch {
      /* keep text */
    }
    if (response.headers.get('content-type')?.includes('text/html')) detail = '';
    throw new Error(detail || `Request failed with status ${response.status}`);
  }

  if (!body) return null;
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

function setConnection(online, detail = '') {
  elements.connectionDot.classList.toggle('online', online);
  elements.connectionDot.classList.toggle('offline', !online);
  elements.connectionLabel.textContent = online ? 'Backend connected' : 'Backend unavailable';
  elements.connectionDetail.textContent =
    detail || (online ? 'API responding' : 'Check app and database');

  elements.apiState.textContent = online ? 'Available' : 'Offline';
  elements.apiState.classList.toggle('ok', online);

  elements.kafkaState.textContent = online ? 'Configured' : 'Unknown';
  elements.kafkaState.classList.toggle('ok', online);
  elements.dbState.textContent = online ? 'Configured' : 'Unknown';
  elements.dbState.classList.toggle('ok', online);

  elements.healthPulse.classList.toggle('offline', !online);
}
function renderRecentEvents(events) {
  if (!events?.length) {
    setEmpty(elements.recentEvents, 4, 'No tracked events yet. Publish an event to get started.');
    return;
  }
  elements.recentEvents.innerHTML = events.slice(0, 5).map((event) => `
    <tr>
      <td class="mono" title="${escapeHtml(event.event_id)}">${escapeHtml(shortId(event.event_id))}</td>
      <td class="mono">${escapeHtml(event.order_id)}</td>
      <td>${statusBadge(event.status)}</td>
      <td>${formatDate(event.updated_at)}</td>
    </tr>
  `).join('');
}

function updateDashboard(eventsPage, ordersPage, dltPage, publishedPage, publishingPage) {
  const totalEvents = Number(eventsPage.totalElements || 0);
  const totalOrders = Number(ordersPage.totalElements || 0);
  const totalDead = Number(dltPage.totalElements || 0);
  const inFlight =
    Number(publishedPage.totalElements || 0) + Number(publishingPage.totalElements || 0);

  elements.metrics.events.textContent = totalEvents.toLocaleString();
  elements.metrics.orders.textContent = totalOrders.toLocaleString();
  elements.metrics.pending.textContent = inFlight.toLocaleString();
  elements.metrics.dead.textContent = totalDead.toLocaleString();
  elements.navDltCount.textContent = totalDead > 99 ? '99+' : String(totalDead);
  renderRecentEvents(eventsPage.content || []);
}

async function loadDashboard() {
  const token = ++state.tokens.dashboard;
  try {
    const [events, orders, dlt, published, publishing] = await Promise.all([
      apiRequest('/api/events?page=0&size=100'),
      apiRequest('/api/orders?page=0&size=1'),
      apiRequest('/api/dead-letter-events?page=0&size=1'),
      apiRequest('/api/events?page=0&size=1&status=PUBLISHED'),
      apiRequest('/api/events?page=0&size=1&status=PUBLISHING')
    ]);
    if (token !== state.tokens.dashboard) return false; // stale

    updateDashboard(events, orders, dlt, published, publishing);
    setConnection(true, 'API responding');
    elements.updated.textContent = `Updated ${new Intl.DateTimeFormat(undefined, {
      hour: 'numeric', minute: '2-digit', second: '2-digit'
    }).format(new Date())}`;
    return true;
  } catch (error) {
    if (token !== state.tokens.dashboard) return false;

    setConnection(false, 'Could not load API data');
    renderRecentEvents([]);
    elements.metrics.events.textContent = '—';
    elements.metrics.orders.textContent = '—';
    elements.metrics.pending.textContent = '—';
    elements.metrics.dead.textContent = '—';
    elements.updated.textContent = 'Refresh to try again';
    if (state.view === 'overview') showToast(error.message, true);
    return false;
  }
}

function setPageButtons(kind, page, totalPages) {
  const { previous, next, label } = pageConfig[kind];
  document.querySelector(previous).disabled = page <= 0;
  document.querySelector(next).disabled = page + 1 >= totalPages;
  document.querySelector(label).textContent = totalPages
    ? `Page ${page + 1} of ${totalPages}`
    : 'Page 1 of 1';
}

function disablePagination(kind) {
  const { previous, next, label } = pageConfig[kind];
  document.querySelector(previous).disabled = true;
  document.querySelector(next).disabled = true;
  document.querySelector(label).textContent = '—';
}

function renderListRow(kind, item) {
  if (kind === 'events') {
    return `<tr>
      <td class="mono" title="${escapeHtml(item.event_id)}">${escapeHtml(shortId(item.event_id))}</td>
      <td class="mono">${escapeHtml(item.order_id)}</td>
      <td>${statusBadge(item.status)}</td>
      <td>${Number(item.attempt_count || 0)}</td>
      <td><span class="truncate" title="${escapeHtml(item.last_error)}">${escapeHtml(item.last_error || '—')}</span></td>
      <td>${formatDate(item.updated_at)}</td>
    </tr>`;
  }
  if (kind === 'orders') {
    return `<tr>
      <td class="mono">${escapeHtml(item.order_id)}</td>
      <td class="mono">${escapeHtml(shortId(item.event_id))}</td>
      <td>${statusBadge(item.status)}</td>
      <td>${formatDate(item.created_at)}</td>
    </tr>`;
  }
  return `<tr>
    <td class="mono" title="${escapeHtml(item.event_id)}">${escapeHtml(shortId(item.event_id))}</td>
    <td class="mono">${escapeHtml(item.order_id)}</td>
    <td>${Number(item.attempt_count || 0)}</td>
    <td><span class="truncate" title="${escapeHtml(item.last_error)}">${escapeHtml(item.last_error || '—')}</span></td>
    <td>${formatDate(item.updated_at)}</td>
  </tr>`;
}

function buildListParams(kind) {
  const params = new URLSearchParams({
    page: String(state[`${kind}Page`]),
    size: String(PAGE_SIZE)
  });
  if (kind === 'events' && state.eventsStatus) params.set('status', state.eventsStatus);
  if (kind === 'orders' && state.ordersStatus) params.set('status', state.ordersStatus);
  return params;
}

async function loadList(kind) {
  const config = pageConfig[kind];
  const token = ++state.tokens[kind];
  const totalLabel = document.querySelector(`#${kind === 'dlt' ? 'dlt' : kind}-total`);

  setLoading(config.body, config.cols);

  try {
    const result = await apiRequest(`${config.endpoint}?${buildListParams(kind)}`);
    if (token !== state.tokens[kind]) return; // stale

    const content = result.content || [];
    if (content.length) {
      config.body.innerHTML = content.map((item) => renderListRow(kind, item)).join('');
    } else {
      setEmpty(config.body, config.cols, `No ${config.noun} found.`);
    }
    totalLabel.textContent = `${Number(result.totalElements || 0).toLocaleString()} ${config.noun}`;
    setPageButtons(kind, result.page || 0, result.totalPages || 0);
  } catch (error) {
    if (token !== state.tokens[kind]) return;

    setEmpty(config.body, config.cols, error.message);
    totalLabel.textContent = 'Unable to load data';
    disablePagination(kind);
    showToast(error.message, true);
  }
}
async function navigate(view) {
  if (!viewTitles[view]) return;

  state.view = view;

  elements.views.forEach((section) => {
    section.classList.toggle('active', section.id === `${view}-view`);
  });

  elements.navItems.forEach((item) => {
    const isActive = item.dataset.viewLink === view;
    item.classList.toggle('active', isActive);
    if (isActive) item.setAttribute('aria-current', 'page');
    else item.removeAttribute('aria-current');
  });

  elements.breadcrumb.textContent = viewTitles[view];
  document.querySelector(`#${view}-view h1[tabindex="-1"]`)?.focus();

  if (view === 'events') await loadList('events');
  if (view === 'orders') await loadList('orders');
  if (view === 'dead-letters') await loadList('dlt');
  if (view === 'overview') await loadDashboard();
  if (view === 'publish') updatePreview();
}

function updatePreview() {
  const payload = {
    orderId: document.querySelector('#order-id').value,
    status: document.querySelector('#order-status').value,
    shouldFail: document.querySelector('#should-fail').checked
  };
  const eventId = document.querySelector('#event-id').value.trim();
  if (eventId) payload.eventId = eventId;
  elements.jsonPreview.textContent = JSON.stringify(payload, null, 2);
}

function submitMarkup() {
  return '<span aria-hidden="true">↑</span> Publish to Kafka';
}
function extractEventId(message) {
  const match = message.match(/\b(?:id|event[_-]?id)\s*[:=]\s*([A-Za-z0-9._-]{6,})/i);
  return match?.[1] || null;
}

async function publishEvent(event) {
  event.preventDefault();
  elements.publishFeedback.textContent = '';
  elements.publishFeedback.classList.remove('error');
  if (!elements.publishForm.reportValidity()) return;

  const payload = {
    orderId: document.querySelector('#order-id').value.trim(),
    status: document.querySelector('#order-status').value,
    shouldFail: document.querySelector('#should-fail').checked
  };
  const eventId = document.querySelector('#event-id').value.trim();
  if (eventId) payload.eventId = eventId;

  elements.publishSubmit.disabled = true;
  elements.publishSubmit.textContent = 'Publishing…';

  try {
    const response = await apiRequest('/api/orders', {
      method: 'POST',
      body: JSON.stringify(payload)
    });

    const message = (typeof response === 'string' && response)
      ? response
      : 'Event accepted by Kafka.';

    elements.publishFeedback.textContent = `${message} Processing may still be in progress.`;
    elements.publishForm.reset();
    updatePreview();
    showToast(message);

    const publishedId = extractEventId(message);
    if (publishedId) {
      document.querySelector('#event-id').placeholder = `Last event: ${publishedId}`;
    }

    await loadDashboard();
  } catch (error) {
    elements.publishFeedback.textContent = error.message;
    elements.publishFeedback.classList.add('error');
    showToast(error.message, true);
  } finally {
    elements.publishSubmit.disabled = false;
    elements.publishSubmit.innerHTML = submitMarkup();
  }
}

document.querySelectorAll('[data-view-link]').forEach((item) => {
  item.addEventListener('click', (event) => {
    event.preventDefault();
    navigate(item.dataset.viewLink);
  });
});

elements.refresh.addEventListener('click', async () => {
  if (elements.refresh.disabled) return;
  elements.refresh.disabled = true;
  try {
    await loadDashboard();
    if (state.view === 'events') await loadList('events');
    if (state.view === 'orders') await loadList('orders');
    if (state.view === 'dead-letters') await loadList('dlt');
  } finally {
    elements.refresh.disabled = false;
  }
});

elements.eventFilter.addEventListener('change', () => {
  state.eventsStatus = elements.eventFilter.value;
  state.eventsPage = 0;
  loadList('events');
});

elements.orderFilter.addEventListener('input', () => {
  clearTimeout(state.orderDebounceTimer);
  state.orderDebounceTimer = setTimeout(() => {
    const value = elements.orderFilter.value.trim();
    if (value === state.ordersStatus) return;
    state.ordersStatus = value;
    state.ordersPage = 0;
    loadList('orders');
  }, ORDER_FILTER_DEBOUNCE);
});

for (const [kind, config] of Object.entries(pageConfig)) {
  document.querySelector(config.previous).addEventListener('click', () => {
    state[`${kind}Page`] = Math.max(0, state[`${kind}Page`] - 1);
    loadList(kind);
  });
  document.querySelector(config.next).addEventListener('click', () => {
    state[`${kind}Page`] += 1;
    loadList(kind);
  });
}

elements.publishForm.addEventListener('submit', publishEvent);
elements.publishForm.querySelectorAll('input, select').forEach((control) => {
  control.addEventListener('input', updatePreview);
  control.addEventListener('change', updatePreview);
});

function refreshCurrentView() {
  const promises = [loadDashboard()];
  if (state.view === 'events') promises.push(loadList('events'));
  if (state.view === 'orders') promises.push(loadList('orders'));
  if (state.view === 'dead-letters') promises.push(loadList('dlt'));
  return Promise.all(promises);
}

function startRefreshTimer() {
  if (state.refreshTimer) return;
  state.refreshTimer = setInterval(refreshCurrentView, REFRESH_INTERVAL);
}

function stopRefreshTimer() {
  clearInterval(state.refreshTimer);
  state.refreshTimer = null;
}

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    stopRefreshTimer();
  } else {
    refreshCurrentView();
    startRefreshTimer();
  }
});

navigate('overview');
startRefreshTimer();