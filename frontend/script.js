        // Dynamically resolve API base URL from current hostname so the
        // dashboard works regardless of whether it's opened via localhost,
        // 127.0.0.1, or a remote host. The backend always runs on port 8000.
        const API_HOST = window.location.hostname || 'localhost';
        const API = `http://${API_HOST}:8000/api/v1/orders`;
        const ordersMap = new Map();

        // ──────────────────────────────────────────────────────────
        // requestAnimationFrame Batching System
        //
        // Instead of calling renderTable() on every SSE event (which
        // would cause 100 full DOM rebuilds during a data tsunami),
        // we buffer all incoming changes and flush them in a single
        // requestAnimationFrame callback (~16ms batching window).
        // ──────────────────────────────────────────────────────────

        let pendingChanges = [];
        let rafScheduled = false;

        /**
         * Queue a batch of WAL changes for the next animation frame.
         * Multiple SSE messages arriving within the same frame are
         * coalesced into a single DOM update.
         */
        function enqueueChanges(changes) {
            pendingChanges.push(...changes);
            if (!rafScheduled) {
                rafScheduled = true;
                requestAnimationFrame(flushPendingChanges);
            }
        }

        /**
         * Process ALL queued changes in one shot, then do a single
         * DOM render + a single summary toast.
         */
        function flushPendingChanges() {
            rafScheduled = false;
            if (pendingChanges.length === 0) return;

            // Drain the buffer
            const batch = pendingChanges.splice(0, pendingChanges.length);

            let inserts = 0, updates = 0, deletes = 0;
            const flashIds = [];

            for (const m of batch) {
                if (m.kind === 'delete') {
                    const pk = m.oldkeys?.keyvalues?.[0];
                    if (pk) {
                        ordersMap.delete(Number(pk));
                        deletes++;
                        logEvent('delete', `Order #${pk} deleted`);
                    }
                } else {
                    const obj = {};
                    m.columnnames.forEach((c, i) => obj[c] = m.columnvalues[i]);
                    obj.id = Number(obj.id);
                    const isNew = !ordersMap.has(obj.id);
                    ordersMap.set(obj.id, obj);

                    if (isNew) {
                        inserts++;
                    } else {
                        updates++;
                    }
                    logEvent(m.kind, `Order #${obj.id} — ${obj.customer_name} — ${obj.status}`);
                    flashIds.push(obj.id);
                }
            }

            // Single DOM render for the entire batch
            renderTable();

            // Flash all new/updated rows
            for (const id of flashIds) {
                flashRow(id);
            }

            // Summary toasts instead of per-event toasts
            if (inserts > 0) showToast(`${inserts} new order${inserts > 1 ? 's' : ''} created`, 'insert');
            if (updates > 0) showToast(`${updates} order${updates > 1 ? 's' : ''} updated`, 'update');
            if (deletes > 0) showToast(`${deletes} order${deletes > 1 ? 's' : ''} deleted`, 'delete');
        }


        // ── Toast Notifications ──────────────────
        const MAX_VISIBLE_TOASTS = 5;

        function showToast(msg, type = 'info') {
            const c = document.getElementById('toast-container');

            // Limit visible toasts to prevent toast flooding
            while (c.children.length >= MAX_VISIBLE_TOASTS) {
                c.firstChild.remove();
            }

            const t = document.createElement('div');
            t.className = 'toast';
            const colors = { insert: '#10b981', update: '#f59e0b', delete: '#ef4444', info: '#38bdf8' };
            t.style.borderLeftColor = colors[type] || colors.info;
            t.style.borderLeftWidth = '3px';
            t.textContent = msg;
            c.appendChild(t);
            setTimeout(() => { t.classList.add('fade-out'); setTimeout(() => t.remove(), 300); }, 3000);
        }

        // ── CDC Event Log ────────────────────────
        const MAX_LOG_ENTRIES = 100;

        function logEvent(kind, detail) {
            const el = document.getElementById('log-entries');
            const ts = new Date().toLocaleTimeString();
            const line = document.createElement('div');
            line.className = `log-entry-${kind}`;
            line.textContent = `[${ts}] ${kind.toUpperCase()}: ${detail}`;
            el.prepend(line);
            // Trim excess entries to prevent unbounded DOM growth
            while (el.children.length > MAX_LOG_ENTRIES) {
                el.lastChild.remove();
            }
        }

        // ── Stats ────────────────────────────────
        function updateStats() {
            let p = 0, s = 0, d = 0;
            ordersMap.forEach(o => { if (o.status === 'pending') p++; else if (o.status === 'shipped') s++; else d++; });
            document.getElementById('stat-total').textContent = ordersMap.size;
            document.getElementById('stat-pending').textContent = p;
            document.getElementById('stat-shipped').textContent = s;
            document.getElementById('stat-delivered').textContent = d;
        }

        // ── Render Table ─────────────────────────
        // Uses DocumentFragment for a single reflow instead of multiple
        // innerHTML assignments. This is critical for tsunami performance.
        function renderTable() {
            const body = document.getElementById('orders-body');
            if (ordersMap.size === 0) {
                body.innerHTML = '<tr id="empty-row"><td colspan="6" class="empty-state">No orders yet. Create one above!</td></tr>';
                updateStats(); return;
            }

            const sorted = [...ordersMap.values()].sort((a, b) => {
                // Handle both Date strings and raw timestamps
                const da = new Date(a.updated_at);
                const db = new Date(b.updated_at);
                return db - da;
            });

            // Build all HTML in a single string, then assign once
            body.innerHTML = sorted.map(o => `
                <tr id="row-${o.id}">
                    <td><strong>#${o.id}</strong></td>
                    <td>${escHtml(o.customer_name)}</td>
                    <td>${escHtml(o.product_name)}</td>
                    <td><span class="badge badge-${o.status}">${o.status}</span></td>
                    <td style="color:var(--text-muted);font-size:0.8rem">${new Date(o.updated_at).toLocaleString()}</td>
                    <td class="actions">
                        ${o.status === 'pending' ? `<button class="btn-action" onclick="updateStatus(${o.id},'shipped')">Ship</button>` : ''}
                        ${o.status === 'shipped' ? `<button class="btn-action" onclick="updateStatus(${o.id},'delivered')">Deliver</button>` : ''}
                        <button class="btn-action danger" onclick="deleteOrder(${o.id})">Delete</button>
                    </td>
                </tr>`).join('');

            updateStats();
        }

        function escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

        function flashRow(id) {
            const row = document.getElementById(`row-${id}`);
            if (row) { row.classList.remove('row-new'); void row.offsetWidth; row.classList.add('row-new'); }
        }

        // ── API Calls ────────────────────────────
        async function loadOrders() {
            try {
                const res = await fetch(API);
                const data = await res.json();
                ordersMap.clear();
                data.forEach(o => ordersMap.set(o.id, o));
                renderTable();
            } catch (e) { console.error('Failed to load orders', e); }
        }

        async function createOrder() {
            const cn = document.getElementById('inp-customer').value.trim();
            const pn = document.getElementById('inp-product').value.trim();
            const st = document.getElementById('inp-status').value;
            if (!cn || !pn) { showToast('Please fill in all fields', 'info'); return; }
            try {
                const res = await fetch(API, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ customer_name: cn, product_name: pn, status: st })
                });
                if (!res.ok) throw new Error(await res.text());
                document.getElementById('inp-customer').value = '';
                document.getElementById('inp-product').value = '';
                showToast(`Order created for ${cn}`, 'insert');
            } catch (e) { showToast('Failed to create order', 'delete'); }
        }

        async function updateStatus(id, status) {
            try {
                const res = await fetch(`${API}/${id}`, {
                    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ status })
                });
                if (!res.ok) throw new Error(await res.text());
            } catch (e) { showToast('Failed to update order', 'delete'); }
        }

        async function deleteOrder(id) {
            try {
                const res = await fetch(`${API}/${id}`, { method: 'DELETE' });
                if (!res.ok) throw new Error(await res.text());
            } catch (e) { showToast('Failed to delete order', 'delete'); }
        }

        // ── SSE Stream ───────────────────────────
        function connectSSE() {
            const es = new EventSource(`${API}/stream`);
            const pulse = document.getElementById('pulse');
            const stxt = document.getElementById('status-text');

            es.onopen = () => {
                pulse.className = 'pulse connected';
                stxt.textContent = 'Connected — listening to PostgreSQL WAL stream';
            };
            es.onerror = () => {
                pulse.className = 'pulse disconnected';
                stxt.textContent = 'Disconnected — reconnecting...';
            };
            es.onmessage = (e) => {
                try {
                    const payload = JSON.parse(e.data);
                    if (!payload.change) return;

                    // Enqueue for batched rendering instead of immediate DOM update
                    enqueueChanges(payload.change);

                } catch (err) { console.error('SSE parse error', err); }
            };
        }

        // ── Init ─────────────────────────────────
        loadOrders();
        connectSSE();