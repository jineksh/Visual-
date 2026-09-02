'use strict';

/* ==========================================================================
   Transaction Lab — simulation engine
   Vanilla JS, no dependencies. Organized as:
     1. Global state + helpers (log, explanation panel, dock state views)
     2. Section registry / navigation
     3. Per-section modules (render + event handlers)
     4. Bootstrapping
   ========================================================================== */

/* ---------------------------------------------------------------------- *
 * 1. GLOBAL STATE
 * ---------------------------------------------------------------------- */

const App = {
  mode: 'generic',              // 'generic' | 'postgres'
  isolation: 'read-committed',  // read-uncommitted | read-committed | repeatable-read | serializable
  speedMs: 800,
  currentSection: 'basics',
  eventLog: [],                 // {time, tag, msg}
  logFilter: 'all',
  playing: false,

  // shared "committed database" used across several sections
  accounts: { A: 1000, B: 1000 },

  // shared low-level machine state, used by dirty-page / WAL / buffer / checkpoint / recovery sections
  machine: {
    page: { id: 42, diskValue: 1000, bufferValue: 1000, dirty: false, cached: true },
    wal: { buffer: [], durable: [], nextLsn: 1 },
    bufferStats: { hits: 0, misses: 0 },
    lastCommitDurable: null,
    crashed: false,
  },
};

function fmtTime() {
  const d = new Date();
  return d.toTimeString().slice(0, 8);
}

/** Push an event to the shared log, tagged by category, and re-render the log dock. */
function logEvent(tag, msg) {
  App.eventLog.push({ time: fmtTime(), tag, msg });
  renderLog();
  renderTimelineDock();
}

/** Update the right-hand Explanation panel. simplified is optional extra text. */
function explain(what, why, interviewPoint, simplified) {
  document.getElementById('expWhat').textContent = what;
  document.getElementById('expWhy').textContent = why;
  document.getElementById('expInterview').textContent = interviewPoint;
  const wrap = document.getElementById('simplifiedNoteWrap');
  if (simplified) {
    wrap.hidden = false;
    document.getElementById('simplifiedNote').textContent = simplified;
  } else {
    wrap.hidden = true;
  }
}

function renderLog() {
  const list = document.getElementById('eventLog');
  const items = App.eventLog.filter(e => App.logFilter === 'all' || e.tag === App.logFilter);
  list.innerHTML = items.map(e => `
    <div class="log-entry">
      <span class="log-time">${e.time}</span>
      <span class="log-tag ${e.tag}">${e.tag}</span>
      <span class="log-msg">${e.msg}</span>
    </div>`).join('');
}

function renderTimelineDock() {
  const track = document.getElementById('timelineTrack');
  track.innerHTML = App.eventLog.slice(-40).map(e =>
    `<div class="timeline-item">${e.time} · <strong>${e.tag}</strong> — ${e.msg}</div>`
  ).join('');
}

function renderDbStateDock() {
  const tbody = document.querySelector('#dbStateTable tbody');
  tbody.innerHTML = Object.entries(App.accounts).map(([k, v]) =>
    `<tr><td>${k}</td><td>₹${v}</td></tr>`).join('');
}

function renderMemDiskDock() {
  const m = App.machine;
  document.getElementById('memStateGrid').innerHTML = `
    <div class="mem-tile ${m.page.dirty ? 'dirty' : ''}">
      Page ${m.page.id} (shared buffer)<br>value = ${m.page.bufferValue}<br>${m.page.dirty ? '<span style="color:var(--c-warning)">DIRTY</span>' : '<span style="color:var(--c-success)">clean</span>'}
    </div>
    <div class="mem-tile">
      WAL buffer<br>${m.wal.buffer.length} record(s) not yet flushed
    </div>`;
  document.getElementById('diskStateGrid').innerHTML = `
    <div class="disk-tile">
      Data file — Page ${m.page.id}<br>value = ${m.page.diskValue}
    </div>
    <div class="disk-tile">
      WAL (durable)<br>${m.wal.durable.length} record(s)
    </div>`;
}

function renderTxStateDock() {
  const el = document.getElementById('txStateCards');
  const cards = [];
  if (window.ConcurrencyModule) cards.push(...window.ConcurrencyModule.txStateCards());
  el.innerHTML = cards.length ? cards.join('') : `<div class="tx-state-card">No active transactions. Visit "Concurrency" to start T1 / T2.</div>`;
}

function refreshDock() {
  renderLog();
  renderTimelineDock();
  renderDbStateDock();
  renderMemDiskDock();
  renderTxStateDock();
}

/** Small helper: sleep respecting global speed, used by animated multi-step sequences. */
function wait(ms) {
  return new Promise(res => setTimeout(res, ms ?? App.speedMs));
}

/* ---------------------------------------------------------------------- *
 * 2. SECTION REGISTRY / NAVIGATION
 * ---------------------------------------------------------------------- */

const Sections = {}; // sectionId -> { render(), step()? }

function goToSection(id) {
  App.currentSection = id;
  document.querySelectorAll('#navList li').forEach(li =>
    li.classList.toggle('active', li.dataset.section === id));
  const tpl = document.getElementById('tpl-' + id);
  const host = document.getElementById('sectionHost');
  host.innerHTML = '';
  host.appendChild(tpl.content.cloneNode(true));
  if (Sections[id] && Sections[id].render) Sections[id].render();
}

/* ---------------------------------------------------------------------- *
 * 3. SECTION MODULES
 * ---------------------------------------------------------------------- */

/* ---------- 01 · Transaction Basics ------------------------------------ */

Sections.basics = (() => {
  let state;
  function fresh() {
    return {
      inTx: false,
      committed: { ...App.accounts },
      working: { ...App.accounts },
      savepoints: [], // {name, snapshot:{A,B}}
      opsLog: [], // {text, undone}
    };
  }
  function renderBalances() {
    document.querySelector('#basicsBeforeA .balance-value').textContent = `₹${state.committed.A}`;
    document.querySelector('#basicsBeforeB .balance-value').textContent = `₹${state.committed.B}`;
    document.querySelector('#basicsWorkA .balance-value').textContent = `₹${state.working.A}`;
    document.querySelector('#basicsWorkB .balance-value').textContent = `₹${state.working.B}`;
    document.getElementById('savepointTrack').innerHTML = state.opsLog.map(o =>
      `<div class="sp-step ${o.undone ? 'undone' : ''}">${o.text}</div>`).join('');
  }

  function flash(elId) {
    const el = document.getElementById(elId);
    el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');
  }

  function render() {
    state = fresh();
    renderBalances();
    document.querySelectorAll('[data-basics]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.basics)));
  }

  function handle(action) {
    switch (action) {
      case 'begin':
        state = fresh();
        state.inTx = true;
        logEvent('Transactions', 'BEGIN — transaction is now ACTIVE, working state copied from committed state');
        explain('A new transaction started.', 'BEGIN opens a transaction. It gets its own working view of the data; nothing durable changes yet.', 'A transaction is a unit of work that is either fully applied or not applied at all.');
        break;
      case 'debit':
        if (!state.inTx) return explain('No active transaction.', 'You must BEGIN before modifying data.', 'Writes outside a transaction still run inside an implicit one on most databases.');
        state.working.A -= 200;
        state.opsLog.push({ text: 'A -= 200' });
        flash('basicsWorkA');
        logEvent('Transactions', 'A -= 200 applied to working state (not yet committed)');
        explain('A was decreased by 200 in the transaction\'s working state.', 'The change is visible to this transaction but not yet durable or visible to others.', 'Uncommitted writes live in the transaction\'s own view until COMMIT or ROLLBACK.');
        break;
      case 'credit':
        if (!state.inTx) return;
        state.working.B += 200;
        state.opsLog.push({ text: 'B += 200' });
        flash('basicsWorkB');
        logEvent('Transactions', 'B += 200 applied to working state (not yet committed)');
        explain('B was increased by 200 in the transaction\'s working state.', 'Both halves of the transfer are staged together before either becomes durable.', 'Grouping related writes in one transaction is what gives you atomicity.');
        break;
      case 'commit':
        if (!state.inTx) return;
        state.committed = { ...state.working };
        App.accounts = { ...state.committed };
        state.inTx = false;
        document.getElementById('basicsWorkA').classList.add('committed');
        document.getElementById('basicsWorkB').classList.add('committed');
        logEvent('Transactions', 'COMMIT — working state becomes the new committed state');
        explain('The transaction committed.', 'All staged writes became durable and visible to other transactions together.', 'COMMIT makes every write in the transaction durable and visible atomically.');
        renderDbStateDock();
        break;
      case 'rollback':
        if (!state.inTx) return;
        state.working = { ...state.committed };
        state.opsLog.forEach(o => o.undone = true);
        state.inTx = false;
        document.getElementById('basicsWorkA').classList.add('rolledback');
        document.getElementById('basicsWorkB').classList.add('rolledback');
        logEvent('Transactions', 'ROLLBACK — working state discarded, committed state unchanged');
        explain('The transaction rolled back.', 'Every staged write in this transaction was discarded; the committed state is untouched.', 'ROLLBACK guarantees an all-or-nothing outcome — partial writes never become visible.');
        break;
      case 'sp-debit100':
        if (!state.inTx) state.inTx = true, state.working = { ...state.committed };
        state.working.A -= 100;
        state.opsLog.push({ text: 'A -= 100' });
        flash('basicsWorkA');
        logEvent('Transactions', 'A -= 100 applied');
        break;
      case 'sp-mark':
        state.savepoints.push({ name: 's1', snapshot: { ...state.working }, opIndex: state.opsLog.length });
        state.opsLog.push({ text: 'SAVEPOINT s1' });
        logEvent('Transactions', 'SAVEPOINT s1 recorded — a named point the transaction can roll back to without aborting entirely');
        explain('Savepoint s1 was marked.', 'A savepoint remembers the working state at this point so later work can be undone without discarding the whole transaction.', 'SAVEPOINT lets you partially roll back a transaction instead of aborting it completely.');
        break;
      case 'sp-debit200':
        state.working.A -= 200;
        state.opsLog.push({ text: 'A -= 200 (after s1)' });
        flash('basicsWorkA');
        logEvent('Transactions', 'A -= 200 applied after savepoint s1');
        break;
      case 'sp-rollback': {
        const sp = state.savepoints.find(s => s.name === 's1');
        if (!sp) return;
        state.working = { ...sp.snapshot };
        state.opsLog.forEach((o, i) => { if (i > sp.opIndex) o.undone = true; });
        logEvent('Transactions', 'ROLLBACK TO SAVEPOINT s1 — only work done after s1 is undone; the transaction stays active');
        explain('Rolled back to savepoint s1.', 'Everything staged after s1 was discarded, but everything before s1 (and the transaction itself) survives.', 'ROLLBACK TO SAVEPOINT undoes recent work while keeping the transaction open for a COMMIT.');
        break;
      }
    }
    renderBalances();
  }

  return { render };
})();

/* ---------- 02 · ACID --------------------------------------------------- */

Sections.acid = (() => {
  function render() {
    document.querySelectorAll('[data-acid]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.acid)));
    renderAtomic([]);
    renderConsistency(2000, 2000, null);
    document.getElementById('acidIsolationViz').innerHTML = `<div class="mini-row pending">Click "Run concurrent demo" — outcome depends on isolation level: <strong>${labelIso(App.isolation)}</strong> (${App.mode})</div>`;
    document.getElementById('acidDurabilityViz').innerHTML = `<div class="mini-row pending">Not run yet.</div>`;
  }

  function renderAtomic(rows) {
    document.getElementById('acidAtomicViz').innerHTML = rows.length ? rows.map(r =>
      `<div class="mini-row ${r.cls}">${r.text}</div>`).join('') : '<div class="mini-row pending">Not run yet.</div>';
  }
  function renderConsistency(a, b, verdict) {
    let html = `<div class="mini-row">A + B = ${a + b}</div>`;
    if (verdict) html += `<div class="mini-row ${verdict === 'ok' ? 'ok' : 'fail'}">${verdict === 'ok' ? 'Invariant holds' : 'Invariant VIOLATED — write rejected'}</div>`;
    document.getElementById('acidConsistencyViz').innerHTML = html;
  }

  async function handle(action) {
    switch (action) {
      case 'atomic-run': {
        renderAtomic([{ text: 'BEGIN', cls: 'pending' }]);
        await wait(300);
        renderAtomic([{ text: 'BEGIN', cls: 'ok' }, { text: 'A -= 200 ... SUCCESS', cls: 'ok' }]);
        await wait(App.speedMs);
        renderAtomic([{ text: 'BEGIN', cls: 'ok' }, { text: 'A -= 200 ... SUCCESS', cls: 'ok' }, { text: 'B += 200 ... FAIL (constraint error)', cls: 'fail' }]);
        await wait(App.speedMs);
        renderAtomic([{ text: 'BEGIN', cls: 'ok' }, { text: 'A -= 200 ... SUCCESS', cls: 'fail' }, { text: 'B += 200 ... FAIL', cls: 'fail' }, { text: 'ROLLBACK — entire transaction undone, including the earlier successful write', cls: 'fail' }]);
        logEvent('Transactions', 'Atomicity demo: second write failed, so the whole transaction (including the earlier successful write) was rolled back');
        explain('One write in the transaction failed.', 'Because both writes were in the same transaction, the database undid the already-succeeded write too, instead of leaving the data half-changed.', 'Atomicity means "all of a transaction\'s writes happen, or none do" — partial application is never visible.');
        break;
      }
      case 'atomic-reset': renderAtomic([]); break;
      case 'consistency-valid':
        renderConsistency(800, 1200, 'ok');
        logEvent('Transactions', 'Valid transfer: A -= 200, B += 200 — total unchanged, invariant preserved');
        explain('A valid transfer ran.', 'The transaction moved value between rows but preserved the invariant A + B = 2000.', 'Consistency means a transaction moves the database from one valid state to another, respecting its declared invariants and constraints.');
        break;
      case 'consistency-invalid':
        renderConsistency(2000, 300, 'fail');
        logEvent('Transactions', 'Attempted write would break invariant A + B = 2000 — database rejects it (e.g. via a CHECK constraint)');
        explain('A write that would break the invariant was rejected.', 'The database enforces consistency through constraints; a transaction that would violate them is not allowed to commit as-is.', 'Consistency is partly enforced by the application\'s transaction logic and partly by database-level constraints.');
        break;
      case 'isolation-demo':
        document.getElementById('acidIsolationViz').innerHTML = `<div class="mini-row pending">T1 and T2 both touch the same row concurrently. See the "Concurrency Problems" section for the exact outcome under <strong>${labelIso(App.isolation)}</strong> (${App.mode} mode).</div>`;
        logEvent('Transactions', 'Isolation demo pointed to the Concurrency Problems section for a concrete run');
        explain('Isolation concerns concurrent transactions.', 'What T2 can see of T1\'s uncommitted or recently-committed work depends entirely on the isolation level.', 'Isolation controls how much of one transaction\'s in-progress work is visible to another concurrent transaction.');
        break;
      case 'durability-demo': {
        const box = document.getElementById('acidDurabilityViz');
        box.innerHTML = `<div class="mini-row ok">COMMIT (WAL flushed durably)</div>`;
        await wait(App.speedMs);
        box.innerHTML += `<div class="mini-row fail">💥 Crash — RAM lost</div>`;
        await wait(App.speedMs);
        box.innerHTML += `<div class="mini-row ok">Recovery replays durable WAL — committed change survives</div>`;
        logEvent('Recovery', 'Durability demo: committed change survived a simulated crash because its WAL record was durable before the crash');
        explain('A committed change survived a crash.', 'Durability is provided by WAL being made durable before commit is acknowledged — recovery replays it after a crash.', 'Durability does not mean the data page was on disk at commit time — it means enough was made durable (WAL) to reconstruct the change.');
        break;
      }
    }
  }
  return { render };
})();

function labelIso(v) {
  return { 'read-uncommitted': 'Read Uncommitted', 'read-committed': 'Read Committed', 'repeatable-read': 'Repeatable Read', 'serializable': 'Serializable' }[v];
}

/* ---------- 03 · Concurrent Transactions -------------------------------- */

Sections.concurrency = (() => {
  let tx, timeline;
  function fresh() {
    return {
      T1: { status: 'idle', local: null, hasRead: false },
      T2: { status: 'idle', local: null, hasRead: false },
    };
  }
  tx = fresh();
  timeline = { T1: [], T2: [] };

  function render() {
    tx = fresh(); timeline = { T1: [], T2: [] };
    renderLanes(); renderTimeline();
    document.querySelectorAll('[data-conc]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.conc)));
  }

  function renderLanes() {
    const el = document.getElementById('concLanes');
    el.innerHTML = ['T1', 'T2'].map(id => {
      const t = tx[id];
      return `<div class="tx-lane">
        <h4><span class="tx-status-dot ${t.status === 'active' ? 'active' : t.status === 'committed' ? 'committed' : t.status === 'aborted' ? 'aborted' : ''}"></span>${id} — ${t.status}</h4>
        <div class="tx-line">local view: ${t.local === null ? '(not read yet)' : '₹' + t.local}</div>
      </div>`;
    }).join('');
  }

  function renderTimeline() {
    const el = document.getElementById('concTimeline');
    el.innerHTML = ['T1', 'T2'].map(id => `
      <div class="timeline-row">
        <span class="tl-label">${id}</span>
        <div class="tl-track">${timeline[id].map(e => `<span class="tl-event ${e.cls}">${e.label}</span>`).join('')}</div>
      </div>`).join('');
  }

  function handle(action) {
    const [id, op] = action.split('-');
    const t = tx[id];
    switch (op) {
      case 'begin':
        if (t.status !== 'idle') return;
        t.status = 'active';
        t.local = null;
        timeline[id].push({ label: 'BEGIN', cls: '' });
        logEvent('Transactions', `${id} BEGIN`);
        explain(`${id} started.`, `${id} is now active and can read/write the shared balance.`, 'Multiple transactions can be active at the same time — that concurrency is exactly what isolation levels manage.');
        break;
      case 'read':
        if (t.status !== 'active') return;
        t.local = App.accounts.A;
        t.hasRead = true;
        timeline[id].push({ label: `READ=${t.local}`, cls: 'read' });
        logEvent('Transactions', `${id} READ balance = ${t.local}`);
        explain(`${id} read the balance.`, `${id} now has its own local view of the value, taken according to the current isolation level's snapshot rules.`, 'Where that snapshot comes from (once per transaction vs. once per statement) is exactly what separates Repeatable Read from Read Committed.');
        break;
      case 'update':
        if (t.status !== 'active' || !t.hasRead) return;
        t.local += 100;
        timeline[id].push({ label: `UPDATE→${t.local}`, cls: 'write' });
        logEvent('Transactions', `${id} UPDATE local balance to ${t.local} (not yet committed)`);
        explain(`${id} modified its local copy.`, `The write is staged in ${id}'s transaction and isn't visible to the other transaction until commit.`, 'Uncommitted writes are private to the writing transaction until COMMIT.');
        break;
      case 'commit':
        if (t.status !== 'active') return;
        App.accounts.A = t.local ?? App.accounts.A;
        t.status = 'committed';
        timeline[id].push({ label: 'COMMIT', cls: 'commit' });
        logEvent('Transactions', `${id} COMMIT — balance is now ${App.accounts.A}`);
        explain(`${id} committed.`, `${id}'s changes are now durable and visible to every other transaction.`, 'A commit makes a transaction\'s effects visible to all subsequently-reading transactions.');
        renderDbStateDock();
        break;
      case 'rollback':
        if (t.status !== 'active') return;
        t.status = 'aborted';
        timeline[id].push({ label: 'ROLLBACK', cls: 'abort' });
        logEvent('Transactions', `${id} ROLLBACK — its local changes are discarded`);
        explain(`${id} rolled back.`, `${id}'s local writes are discarded and never affect the shared balance.`, 'A rolled-back transaction has zero effect on the database, as if it never ran.');
        break;
    }
    renderLanes(); renderTimeline();
  }

  return { render, txStateCards: () => ['T1', 'T2'].filter(id => tx[id].status !== 'idle').map(id =>
    `<div class="tx-state-card"><div class="tsc-title">${id}</div>status: ${tx[id].status}<br>local: ${tx[id].local ?? '—'}</div>`) };
})();

/* ---------- 04 · Isolation Levels + 05 Concurrency Problems ------------- */

// canonical anomaly matrix: [level][mode] -> {dirty, nonrepeatable, phantom} booleans = "possible"
const IsoMatrix = {
  'read-uncommitted': {
    generic:  { dirty: true,  nonrepeatable: true,  phantom: true },
    postgres: { dirty: false, nonrepeatable: true,  phantom: true }, // PG: read uncommitted behaves like read committed
  },
  'read-committed': {
    generic:  { dirty: false, nonrepeatable: true, phantom: true },
    postgres: { dirty: false, nonrepeatable: true, phantom: true },
  },
  'repeatable-read': {
    generic:  { dirty: false, nonrepeatable: false, phantom: true },  // ANSI standard permits phantoms here
    postgres: { dirty: false, nonrepeatable: false, phantom: false }, // PG's snapshot-based Repeatable Read also blocks phantoms
  },
  'serializable': {
    generic:  { dirty: false, nonrepeatable: false, phantom: false },
    postgres: { dirty: false, nonrepeatable: false, phantom: false },
  },
};

Sections.isolation = (() => {
  function render() {
    const tbody = document.querySelector('#isoMatrix tbody');
    tbody.innerHTML = Object.keys(IsoMatrix).map(level => {
      const row = IsoMatrix[level][App.mode];
      const pill = v => `<span class="pill ${v ? 'possible' : 'prevented'}">${v ? 'possible' : 'prevented'}</span>`;
      return `<tr class="${level === App.isolation ? 'current-row' : ''}">
        <td>${labelIso(level)}</td><td>${pill(row.dirty)}</td><td>${pill(row.nonrepeatable)}</td><td>${pill(row.phantom)}</td>
      </tr>`;
    }).join('');
    const note = document.getElementById('isoPgNote');
    note.textContent = App.mode === 'postgres'
      ? "PostgreSQL note: Read Uncommitted is accepted syntactically but behaves exactly like Read Committed — PostgreSQL never actually shows uncommitted data. Repeatable Read in PostgreSQL is snapshot-based (via MVCC) and, unlike the ANSI baseline, also prevents phantom reads in practice."
      : "Generic mode shows the textbook ANSI SQL isolation matrix, which is what most database courses teach — real engines vary in exactly how they implement each level.";
    explain('Isolation matrix rendered.', `Showing anomalies for ${App.mode} mode.`, 'Isolation levels trade correctness guarantees for concurrency: stricter levels prevent more anomalies but allow less concurrency (and can force retries under contention).');
  }
  return { render };
})();

Sections.problems = (() => {
  function render() {
    document.getElementById('dirtyReadViz').innerHTML = '';
    document.getElementById('nonRepeatableViz').innerHTML = '';
    document.getElementById('phantomViz').innerHTML = '';
    document.querySelectorAll('[data-prob]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.prob)));
  }

  function outcomeLine(possible) {
    return possible
      ? `<div class="mini-row fail">ANOMALY OCCURS under ${labelIso(App.isolation)} (${App.mode})</div>`
      : `<div class="mini-row ok">Prevented under ${labelIso(App.isolation)} (${App.mode})</div>`;
  }

  async function handle(action) {
    const anomaly = IsoMatrix[App.isolation][App.mode];
    if (action === 'dirty-read') {
      const el = document.getElementById('dirtyReadViz');
      el.innerHTML = '<div class="mini-row pending">T1: UPDATE X = 500 (uncommitted)</div>';
      await wait(App.speedMs);
      const seen = anomaly.dirty;
      el.innerHTML += `<div class="mini-row ${seen ? 'fail' : 'ok'}">T2: READS X = ${seen ? '500 (uncommitted!)' : '<original value> (committed only)'}</div>`;
      await wait(App.speedMs);
      el.innerHTML += '<div class="mini-row">T1: ROLLBACK</div>' + outcomeLine(seen);
      logEvent('Transactions', `Dirty read scenario under ${labelIso(App.isolation)}/${App.mode}: ${seen ? 'occurred' : 'prevented'}`);
      explain('Dirty read scenario ran.', seen
        ? 'T2 saw a value that T1 later rolled back — an uncommitted, ultimately-invalid value.'
        : 'This isolation level only lets transactions see committed data, so T2 never saw T1\'s uncommitted write.',
        'A dirty read is seeing another transaction\'s uncommitted data. It requires Read Uncommitted semantics — which PostgreSQL never actually provides, even when you request that level.');
    }
    if (action === 'non-repeatable') {
      const el = document.getElementById('nonRepeatableViz');
      el.innerHTML = '<div class="mini-row">T1: READS X = 100</div>';
      await wait(App.speedMs);
      el.innerHTML += '<div class="mini-row">T2: UPDATE X = 200, COMMIT</div>';
      await wait(App.speedMs);
      const changed = anomaly.nonrepeatable;
      el.innerHTML += `<div class="mini-row ${changed ? 'fail' : 'ok'}">T1: READS X AGAIN = ${changed ? '200 (changed!)' : '100 (same, from T1\'s snapshot)'}</div>` + outcomeLine(changed);
      logEvent('Transactions', `Non-repeatable read scenario under ${labelIso(App.isolation)}/${App.mode}: ${changed ? 'occurred' : 'prevented'}`);
      explain('Non-repeatable read scenario ran.', changed
        ? 'T1 re-read the row and got a different value because it re-checks committed state on every read.'
        : 'T1 holds a stable snapshot for its duration, so the same row reads the same value throughout the transaction.',
        'Non-repeatable read: re-reading the same row within one transaction returns a different value because another transaction committed a change in between.');
    }
    if (action === 'phantom') {
      const el = document.getElementById('phantomViz');
      el.innerHTML = '<div class="mini-row">T1: SELECT ... WHERE salary &gt; 50000 → 3 rows</div>';
      await wait(App.speedMs);
      el.innerHTML += '<div class="mini-row">T2: INSERT a row with salary &gt; 50000, COMMIT</div>';
      await wait(App.speedMs);
      const appeared = anomaly.phantom;
      el.innerHTML += `<div class="mini-row ${appeared ? 'fail' : 'ok'}">T1: SAME QUERY AGAIN → ${appeared ? '4 rows (a new row appeared!)' : '3 rows (unchanged)'}</div>` + outcomeLine(appeared);
      logEvent('Transactions', `Phantom read scenario under ${labelIso(App.isolation)}/${App.mode}: ${appeared ? 'occurred' : 'prevented'}`);
      explain('Phantom read scenario ran.', appeared
        ? 'A new row matching T1\'s WHERE clause became visible on re-query because this level does not lock or snapshot against inserts.'
        : 'This level prevents new matching rows from appearing to a re-run query within the same transaction.',
        'Phantom read: a repeated range query returns a different set of rows because another transaction inserted (or deleted) a matching row and committed.');
    }
  }
  return { render };
})();

/* ---------- 06 · Locks --------------------------------------------------- */

Sections.locks = (() => {
  let state;
  function fresh() { return { sharedHolders: [], exclusiveOwner: null, waiters: [] }; }
  state = fresh();

  function render() {
    state = fresh();
    renderViz();
    document.querySelectorAll('[data-lock]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.lock)));
  }

  function renderViz() {
    const el = document.getElementById('lockRowViz');
    el.innerHTML = `
      <div class="lock-line">Row 1</div>
      <div class="lock-line">Shared holders: ${state.sharedHolders.length ? state.sharedHolders.join(', ') : '(none)'}</div>
      <div class="lock-line">Exclusive owner: ${state.exclusiveOwner ? `<span class="lock-owner">${state.exclusiveOwner}</span>` : '(none)'}</div>
      <div class="lock-line">Waiting: ${state.waiters.length ? state.waiters.map(w => `<span class="lock-waiter">${w}</span>`).join(', ') : '(none)'}</div>`;
  }

  function releaseAndPromote() {
    // simplified: first waiter (if any) acquires exclusively once row is free
    if (!state.exclusiveOwner && state.sharedHolders.length === 0 && state.waiters.length) {
      const next = state.waiters.shift();
      state.exclusiveOwner = next;
      logEvent('Locks', `${next} was waiting; lock now free, so ${next} acquires it and continues`);
    }
  }

  function handle(action) {
    switch (action) {
      case 't1-read-shared':
        if (state.exclusiveOwner) { state.waiters.push('T1'); logEvent('Locks', 'T1 requests shared lock — row is exclusively locked, T1 waits'); break; }
        state.sharedHolders.push('T1');
        logEvent('Locks', 'T1 acquires a SHARED lock for reading');
        explain('T1 acquired a shared lock.', 'Shared (read) locks are compatible with other shared locks — multiple readers can hold one at once.', 'Shared locks allow concurrent readers but block concurrent writers.');
        break;
      case 't2-read-shared':
        if (state.exclusiveOwner) { state.waiters.push('T2'); logEvent('Locks', 'T2 requests shared lock — row is exclusively locked, T2 waits'); break; }
        state.sharedHolders.push('T2');
        logEvent('Locks', 'T2 acquires a SHARED lock for reading (compatible with T1\'s shared lock)');
        explain('T2 also acquired a shared lock.', 'Because shared locks are mutually compatible, T2 does not need to wait for T1.', 'Two transactions can both read the same row concurrently under a shared lock.');
        break;
      case 't1-write-exclusive':
        if (state.exclusiveOwner || state.sharedHolders.some(h => h !== 'T1')) {
          state.waiters.push('T1');
          logEvent('Locks', 'T1 requests an EXCLUSIVE lock but the row is already held incompatibly — T1 WAITS');
          explain('T1 must wait.', 'An exclusive lock is incompatible with any other lock, shared or exclusive, held by another transaction.', 'Exclusive (write) locks require sole ownership — no concurrent readers or writers.');
        } else {
          state.sharedHolders = state.sharedHolders.filter(h => h !== 'T1');
          state.exclusiveOwner = 'T1';
          logEvent('Locks', 'T1 acquires an EXCLUSIVE lock and updates the row');
          explain('T1 upgraded to an exclusive lock.', 'With no incompatible holders present, T1 can lock the row exclusively to write it.', 'The actual lock modes and upgrade rules are database-specific; this shows the general shared/exclusive concept.');
        }
        break;
      case 't2-write-exclusive':
        if (state.exclusiveOwner || state.sharedHolders.length) {
          state.waiters.push('T2');
          logEvent('Locks', 'T2 requests an EXCLUSIVE lock — row is held, T2 WAITS');
          explain('T2 is now waiting.', 'T2 cannot proceed until every incompatible lock on the row is released.', 'A lock wait resolves on its own as soon as the current holder commits or rolls back — no special intervention needed.');
        } else {
          state.exclusiveOwner = 'T2';
          logEvent('Locks', 'T2 acquires an EXCLUSIVE lock and updates the row');
        }
        break;
      case 't1-commit':
        if (state.exclusiveOwner === 'T1') state.exclusiveOwner = null;
        state.sharedHolders = state.sharedHolders.filter(h => h !== 'T1');
        logEvent('Locks', 'T1 COMMIT — its locks are released');
        releaseAndPromote();
        explain('T1 committed and released its locks.', 'Any transaction waiting on T1\'s lock can now proceed.', 'Locks are held for the duration of the transaction and released at COMMIT or ROLLBACK.');
        break;
      case 'reset':
        state = fresh();
        logEvent('Locks', 'Lock demo reset');
        break;
    }
    renderViz();
  }
  return { render };
})();

/* ---------- 07 · Deadlocks ------------------------------------------------ */

Sections.deadlocks = (() => {
  let phase = 0; // 0 idle, 1 locked, 2 waiting, 3 deadlocked, 4 resolved
  function render() {
    phase = 0;
    renderGraph();
    document.querySelectorAll('[data-dead]').forEach(btn =>
      btn.addEventListener('click', () => handle(btn.dataset.dead)));
  }
  function renderGraph() {
    const el = document.getElementById('deadlockGraph');
    const t1Class = phase >= 3 ? (phase === 4 ? '' : 'deadlocked') : phase >= 2 ? 'waiting' : '';
    const t2Class = phase >= 3 ? (phase === 4 ? 'aborted' : 'deadlocked') : phase >= 2 ? 'waiting' : '';
    el.innerHTML = `
      <div class="dl-node ${t1Class}">T1<br><small>owns A</small></div>
      <div class="dl-arrow">${phase >= 2 ? '⇄ requests each other\'s row ⇄' : '—'}</div>
      <div class="dl-node ${t2Class}">T2<br><small>owns B</small></div>`;
    let status = '';
    if (phase === 1) status = '<div class="dl-status">T1 locked Row A, T2 locked Row B.</div>';
    if (phase === 2) status = '<div class="dl-status">T1 requests Row B (WAIT). T2 requests Row A (WAIT).</div>';
    if (phase === 3) status = '<div class="dl-status danger">DEADLOCK DETECTED — circular wait, neither can proceed.</div>';
    if (phase === 4) status = '<div class="dl-status success">T2 aborted by the database; T1 acquires Row B and continues.</div>';
    document.querySelectorAll('.dl-status').forEach(n => n.remove());
    el.insertAdjacentHTML('afterend', status);
  }
  async function handle(action) {
    if (action === 'reset') { phase = 0; renderGraph(); logEvent('Locks', 'Deadlock demo reset'); return; }
    if (action === 'run') {
      phase = 1; renderGraph();
      logEvent('Locks', 'T1 locks Row A. T2 locks Row B.');
      await wait(App.speedMs);
      phase = 2; renderGraph();
      logEvent('Locks', 'T1 requests Row B → WAIT. T2 requests Row A → WAIT.');
      await wait(App.speedMs);
      phase = 3; renderGraph();
      logEvent('Locks', 'Database\'s deadlock detector finds a cycle in the wait-for graph: DEADLOCK DETECTED');
      explain('A deadlock was detected.', 'T1 waits for a resource T2 holds, and T2 waits for one T1 holds — neither can ever proceed unassisted.', 'A deadlock is a circular wait between transactions; unlike an ordinary lock wait, it cannot resolve on its own and the database must intervene.');
      return;
    }
    if (action === 'resolve') {
      if (phase !== 3) return;
      phase = 4; renderGraph();
      logEvent('Locks', 'Database aborts T2 (the "deadlock victim") and rolls it back, releasing Row B; T1 then acquires it and continues');
      explain('The deadlock was resolved.', 'The database picked one transaction to abort so the cycle is broken and the survivor can make progress.', 'After a deadlock, the aborted transaction should be retried by the application — it was rolled back, not silently skipped.');
    }
  }
  return { render };
})();

/* ---------- 08 · Optimistic Locking --------------------------------------- */

Sections.optimistic = (() => {
  let row, t1, t2;
  function fresh() {
    row = { balance: 1000, version: 5 };
    t1 = { read: null };
    t2 = { read: null };
  }
  fresh();

  function render() { fresh(); draw(); document.querySelectorAll('[data-opt]').forEach(b => b.addEventListener('click', () => handle(b.dataset.opt))); }

  function draw() {
    document.getElementById('optimisticViz').innerHTML = `
      <div class="ov-row">
        <div class="ov-card"><h4>Row (current)</h4>id = 1<br>balance = ${row.balance}<br>version = ${row.version}</div>
        <div class="ov-card"><h4>T1's read</h4>${t1.read ? `balance = ${t1.read.balance}<br>version = ${t1.read.version}` : '(not read yet)'}</div>
        <div class="ov-card"><h4>T2's read</h4>${t2.read ? `balance = ${t2.read.balance}<br>version = ${t2.read.version}` : '(not read yet)'}</div>
      </div>
      <div id="ovResult"></div>`;
  }
  function result(html) { document.getElementById('ovResult').innerHTML = `<div class="ov-result ${html.ok ? 'ok' : 'fail'}">${html.text}</div>`; }

  function handle(action) {
    switch (action) {
      case 't1-read': t1.read = { ...row }; logEvent('MVCC', 'T1 reads row: balance=' + row.balance + ', version=' + row.version); break;
      case 't2-read': t2.read = { ...row }; logEvent('MVCC', 'T2 reads row: balance=' + row.balance + ', version=' + row.version); break;
      case 't1-update':
        if (!t1.read) return;
        if (t1.read.version === row.version) {
          row.balance -= 100; row.version += 1;
          logEvent('MVCC', `T1 UPDATE ... SET balance=${row.balance}, version=${row.version} WHERE version=${t1.read.version} → 1 row updated`);
          result({ ok: true, text: '1 row updated — T1\'s write succeeded, version is now ' + row.version });
          explain('T1\'s optimistic update succeeded.', 'The WHERE version = <read version> clause matched, because no one else had changed the row since T1 read it.', 'Optimistic locking checks, at write time, that the row hasn\'t changed since it was read — usually via a version or timestamp column.');
        } else {
          result({ ok: false, text: '0 rows updated — CONFLICT DETECTED' });
        }
        break;
      case 't2-update':
        if (!t2.read) return;
        if (t2.read.version === row.version) {
          row.balance -= 200; row.version += 1;
          logEvent('MVCC', `T2 UPDATE ... WHERE version=${t2.read.version} → 1 row updated`);
          result({ ok: true, text: '1 row updated — T2\'s write succeeded' });
        } else {
          logEvent('MVCC', `T2 UPDATE ... SET balance=800, version=${t2.read.version + 1} WHERE version=${t2.read.version} → 0 rows updated (current version is ${row.version})`);
          result({ ok: false, text: `0 rows updated — CONFLICT DETECTED. The row's version moved from ${t2.read.version} to ${row.version} since T2 read it.` });
          explain('T2\'s optimistic update failed.', 'T2\'s WHERE clause targeted the version it originally read, but that version no longer matches — someone else committed first.', 'A zero-row update under optimistic locking means "someone else got there first" — the application should re-read and retry, not silently ignore it.');
        }
        break;
      case 't2-retry':
        t2.read = { ...row };
        logEvent('MVCC', 'T2 re-reads the row (refreshing its version) before retrying the update');
        explain('T2 refreshed its read.', 'Retrying after refreshing the version is the standard way to recover from an optimistic-locking conflict.', 'Optimistic locking pushes conflict handling (retry logic) into the application rather than blocking upfront.');
        break;
      case 'reset': fresh(); document.getElementById('optimisticViz').innerHTML = ''; break;
    }
    draw();
  }
  return { render };
})();

/* ---------- 09 · Pessimistic Locking --------------------------------------- */

Sections.pessimistic = (() => {
  let owner, waiting, updated;
  function fresh() { owner = null; waiting = []; updated = false; }
  fresh();

  function render() { fresh(); draw(); document.querySelectorAll('[data-pess]').forEach(b => b.addEventListener('click', () => handle(b.dataset.pess))); }
  function draw() {
    document.getElementById('pessimisticViz').innerHTML = `
      <div>Row lock owner: ${owner ? `<strong style="color:var(--c-success)">${owner}</strong>` : '(none)'}</div>
      <div>Waiting: ${waiting.length ? waiting.join(', ') : '(none)'}</div>
      <div>Row updated: ${updated ? 'yes' : 'no'}</div>`;
  }
  function handle(action) {
    switch (action) {
      case 't1-for-update':
        if (owner) { waiting.push('T1'); logEvent('Locks', 'T1: SELECT ... FOR UPDATE — row already locked, T1 WAITS'); break; }
        owner = 'T1';
        logEvent('Locks', 'T1: SELECT ... FOR UPDATE acquires the row lock immediately');
        explain('T1 acquired a pessimistic lock upfront.', 'FOR UPDATE locks the row at read time, before any modification, on the assumption a conflict is likely.', 'Pessimistic locking trades some concurrency for the guarantee that no other transaction can modify the row until you\'re done.');
        break;
      case 't2-for-update':
        if (owner) { waiting.push('T2'); logEvent('Locks', 'T2: SELECT ... FOR UPDATE — row locked by T1, T2 WAITS'); explain('T2 is blocked.', 'T2 requested the same row FOR UPDATE while T1 still holds the lock, so T2 must wait.', 'FOR UPDATE readers block each other on the same row — that\'s the point of pessimistic locking.'); break; }
        owner = 'T2';
        logEvent('Locks', 'T2: SELECT ... FOR UPDATE acquires the row lock');
        break;
      case 't1-update':
        if (owner !== 'T1') return;
        updated = true;
        logEvent('Locks', 'T1: UPDATE — safe, because T1 holds the lock and no one else can be modifying this row');
        break;
      case 't1-commit':
        if (owner !== 'T1') return;
        owner = null;
        logEvent('Locks', 'T1: COMMIT — lock released');
        if (waiting.length) { const n = waiting.shift(); owner = n; logEvent('Locks', `${n} was waiting on FOR UPDATE; it now acquires the lock and continues`); }
        break;
      case 'reset': fresh(); break;
    }
    draw();
  }
  return { render };
})();

/* ---------- 10 · MVCC ------------------------------------------------------ */

Sections.mvcc = (() => {
  let versions, t1Snapshot;
  function fresh() {
    versions = [{ id: 'v1', value: 100, creator: 'T0', status: 'live' }];
    t1Snapshot = null;
  }
  fresh();

  function render() { fresh(); draw(); document.querySelectorAll('[data-mvcc]').forEach(b => b.addEventListener('click', () => handle(b.dataset.mvcc))); }

  function draw() {
    const liveOrAll = versions.map(v => `
      <div class="mvcc-version-card ${v.status}">
        <span class="v-tag">${v.id}</span> value = ${v.value}<br>
        created by ${v.creator} · ${v.status}
      </div>`).join('');
    document.getElementById('mvccVersions').innerHTML = `
      <div class="mvcc-col"><h4>Row 1 — all versions</h4>${liveOrAll}</div>
      <div class="mvcc-col"><h4>T1's view</h4>
        ${t1Snapshot === null ? '<div class="mvcc-version-card">T1 has not started</div>' :
          `<div class="mvcc-version-card live">T1 snapshot taken at ${t1Snapshot}<br>T1 currently sees: <strong>${visibleToT1()}</strong></div>`}
      </div>`;
  }

  function visibleToT1() {
    if (t1Snapshot === null) return '—';
    // Under read committed, T1 re-checks on every statement and sees latest committed version.
    // Under repeatable read / serializable, T1 keeps seeing the version live at BEGIN time.
    if (App.isolation === 'read-uncommitted' || App.isolation === 'read-committed') {
      const live = versions.find(v => v.status === 'live') || versions[versions.length - 1];
      return `${live.id} (value=${live.value})`;
    }
    const snapVersion = versions.find(v => v.id === t1Snapshot);
    return `${snapVersion.id} (value=${snapVersion.value}, its own snapshot)`;
  }

  function handle(action) {
    switch (action) {
      case 't1-begin':
        t1Snapshot = versions.find(v => v.status === 'live').id;
        logEvent('MVCC', `T1 BEGIN — takes a snapshot; the currently-live version is ${t1Snapshot}`);
        explain('T1 took a snapshot.', 'PostgreSQL MVCC gives each transaction a consistent view based on what was committed at (or before, depending on isolation level) the time it starts.', 'A "snapshot" is the set of row versions a transaction is allowed to see — not a physical copy of the whole database.');
        break;
      case 't2-update': {
        const live = versions.find(v => v.status === 'live');
        live.status = 'dead';
        const newId = 'v' + (versions.length + 1);
        versions.push({ id: newId, value: 200, creator: 'T2', status: 'live' });
        logEvent('MVCC', `T2 UPDATE value=200, COMMIT — creates new version ${newId}; old version ${live.id} becomes a dead (obsolete) tuple, not deleted immediately`);
        explain('T2 committed a new row version.', 'Conceptually, PostgreSQL adds a new tuple version rather than overwriting the old one in place; the old version lingers until VACUUM can reclaim it.', 'Do not say PostgreSQL "overwrites the tuple" — it creates a new version and marks the old one dead once it is no longer visible to any transaction.');
        break;
      }
      case 't1-read':
        if (t1Snapshot === null) return;
        logEvent('MVCC', `T1 reads and sees ${visibleToT1()} — determined by isolation level ${labelIso(App.isolation)}`);
        explain('T1 read the row.', App.isolation === 'read-committed'
          ? 'Under Read Committed, each statement gets a fresh snapshot, so T1 sees T2\'s committed change.'
          : 'Under this isolation level, T1 keeps the snapshot it took at BEGIN, so it continues to see the version that was live back then, even though T2 has since committed a newer one.',
          'Which version a transaction sees is governed by visibility rules tied to its snapshot and the current isolation level — this is the mechanism, not just a rule to memorize.');
        break;
      case 'reset': fresh(); break;
    }
    draw();
  }
  return { render };
})();

/* ---------- 11 · Dirty Pages ------------------------------------------------ */

Sections.dirtypages = (() => {
  function render() { draw(); document.querySelectorAll('[data-dp]').forEach(b => b.addEventListener('click', () => handle(b.dataset.dp))); }
  function draw() {
    const m = App.machine.page;
    document.getElementById('dirtyPageViz').innerHTML = `
      <div class="flow-tier">
        <div class="flow-box"><div class="fb-title">Disk — data file, page ${m.id}</div>balance = ${m.diskValue}</div>
        <div class="flow-box ${m.dirty ? 'dirty' : 'clean'}"><div class="fb-title">RAM — shared buffer, page ${m.id}</div>balance = ${m.bufferValue}
          <div class="status-chip ${m.dirty ? 'no' : 'yes'}">${m.dirty ? 'DIRTY' : 'clean (matches disk)'}</div>
        </div>
      </div>`;
  }
  function handle(action) {
    const m = App.machine.page;
    if (action === 'update') {
      m.bufferValue = 900; m.dirty = true;
      logEvent('Pages', `UPDATE balance=900 modifies page ${m.id} in the shared buffer only; page marked DIRTY`);
      explain('Page 42 became dirty.', 'The in-memory copy now differs from the durable on-disk copy — that mismatch is exactly what "dirty" means.', 'A dirty page is a modified in-memory page whose durable data-file copy has not yet been updated — it is not a corrupted page.');
    }
    if (action === 'flush') {
      if (!m.dirty) { explain('Nothing to flush.', 'The page is already clean.', 'Flushing a clean page is a no-op.'); return; }
      m.diskValue = m.bufferValue; m.dirty = false;
      logEvent('Pages', `Dirty page ${m.id} flushed — disk now matches the shared buffer`);
      explain('The dirty page was flushed to disk.', 'The buffer manager (at checkpoint time, buffer eviction, or background writer activity) wrote the page back to its data file.', 'A page flush is a separate event from WAL flush and from COMMIT — a page can stay dirty in RAM well after its change is committed and durable via WAL.');
    }
    if (action === 'reset') { m.diskValue = 1000; m.bufferValue = 1000; m.dirty = false; }
    draw(); renderMemDiskDock();
  }
  return { render };
})();

/* ---------- 12 · Shared Buffers ---------------------------------------------- */

Sections.sharedbuffers = (() => {
  function render() { draw(); document.querySelectorAll('[data-sb]').forEach(b => b.addEventListener('click', () => handle(b.dataset.sb))); }
  function draw() {
    document.getElementById('bufferFlowViz').innerHTML = `
      <div class="flow-tier">
        <div class="flow-box">Application</div>
        <div class="flow-box">PostgreSQL</div>
        <div class="flow-box ${App.machine.page.cached ? 'clean' : 'pending'}">Shared Buffers (RAM)</div>
        <div class="flow-box">Disk / Data Files</div>
      </div>`;
    document.getElementById('bufHits').textContent = App.machine.bufferStats.hits;
    document.getElementById('bufMisses').textContent = App.machine.bufferStats.misses;
  }
  function handle(action) {
    if (action === 'query-cached') {
      App.machine.page.cached = true;
      App.machine.bufferStats.hits++;
      logEvent('Pages', 'Query needs page 42 — already resident in shared buffers: BUFFER HIT');
      explain('A shared buffer hit.', 'The page was already cached in RAM, so PostgreSQL used it directly with no disk read.', 'A buffer hit avoids disk I/O entirely — this is the main reason shared_buffers sizing matters for performance.');
    }
    if (action === 'query-uncached') {
      App.machine.bufferStats.misses++;
      logEvent('Pages', 'Query needs a page not resident in shared buffers: BUFFER MISS — reading from storage');
      explain('A shared buffer miss.', 'The page had to be read from the data file into a shared buffer before it could be used.', 'A buffer miss costs a disk (or OS page cache) read — but that does not mean literally "every page access causes disk I/O", since most access patterns hit cache after warm-up.');
      App.machine.page.cached = true;
    }
    if (action === 'reset') { App.machine.bufferStats = { hits: 0, misses: 0 }; }
    draw();
  }
  return { render };
})();

/* ---------- 13 · WAL ---------------------------------------------------------- */

Sections.wal = (() => {
  function render() { draw(); document.querySelectorAll('[data-wal]').forEach(b => b.addEventListener('click', () => handle(b.dataset.wal))); }
  function draw() {
    const w = App.machine.wal;
    document.getElementById('walFlowViz').innerHTML = `
      <div class="flow-tier">
        <div class="flow-box pending"><div class="fb-title">WAL buffer (RAM)</div>${w.buffer.map(r => walLine(r)).join('') || '(empty)'}</div>
        <div class="flow-box ${w.durable.length ? 'durable' : ''}"><div class="fb-title">WAL segment (durable storage)</div>${w.durable.map(r => walLine(r)).join('') || '(empty)'}</div>
      </div>`;
  }
  function walLine(r) { return `<div>LSN ${r.lsn} · TX ${r.tx} · ${r.text}</div>`; }
  function handle(action) {
    const w = App.machine.wal;
    if (action === 'generate') {
      const rec = { lsn: '0/' + (16000 + w.nextLsn).toString(16).toUpperCase(), tx: 101, text: 'UPDATE accounts SET balance 1000→900' };
      w.nextLsn++; w.buffer.push(rec);
      logEvent('WAL', `WAL record generated: LSN ${rec.lsn}, describing the change (not a full page copy)`);
      explain('A WAL record was generated.', 'The record captures enough information to redo this change during recovery — it is not a duplicate of the whole page.', 'WAL stores the information needed to redo/undo changes; treating it as "a copy of the whole database" is a common but incorrect simplification.');
    }
    if (action === 'flush') {
      if (!w.buffer.length) return;
      w.durable.push(...w.buffer.splice(0));
      logEvent('WAL', 'WAL buffer flushed to durable storage — these records can now survive a crash');
      explain('WAL was flushed.', 'Flushing moves records from the in-memory WAL buffer to durable storage, typically required before a transaction\'s COMMIT can be acknowledged.', 'This is a simplified educational representation of WAL, not PostgreSQL\'s exact binary format.');
    }
    if (action === 'reset') { w.buffer = []; w.durable = []; w.nextLsn = 1; }
    draw(); renderMemDiskDock();
  }
  return { render };
})();

/* ---------- 14 · WAL + Commit detailed steps ---------------------------------- */

Sections.walcommit = (() => {
  const steps = [
    'Find the required page',
    'Page is loaded into shared buffers (if not already cached)',
    'Page is modified in memory',
    'A WAL record describing the change is generated',
    'WAL is flushed to durable storage (required before commit can be acknowledged)',
    'COMMIT is acknowledged to the client',
    'The dirty data page may still be written to disk later (e.g. at checkpoint)',
  ];
  let idx;
  function render() { idx = -1; draw(); document.querySelectorAll('[data-wc]').forEach(b => b.addEventListener('click', () => handle(b.dataset.wc))); }
  function draw() {
    document.getElementById('wcSteps').innerHTML = steps.map((s, i) =>
      `<li class="${i < idx ? 'done' : i === idx ? 'current' : ''}">${s}</li>`).join('');
    document.getElementById('wcStatus').textContent = idx < 0 ? 'Not started.' :
      idx >= steps.length - 1 ? 'Sequence complete. Note: the write-ahead rule requires WAL to reach durable storage before the corresponding data page is durably written — not that WAL always precedes every in-memory change.' :
      `Step ${idx + 1} of ${steps.length}.`;
  }
  async function handle(action) {
    if (action === 'reset') { idx = -1; draw(); return; }
    if (action === 'run-step') { advance(); return; }
    if (action === 'run-all') { while (idx < steps.length - 1) { advance(); await wait(App.speedMs); } }
  }
  function advance() {
    if (idx >= steps.length - 1) return;
    idx++;
    draw();
    logEvent('WAL', `UPDATE flow step ${idx + 1}: ${steps[idx]}`);
    explain(steps[idx], stepWhy(idx), 'Durable commit does not necessarily mean the modified page has already reached its final location in the data file — WAL durability is what makes the commit safe.',
      idx === 4 ? 'The exact ordering guarantee is: required WAL reaches durable storage before the corresponding data page is durably written in a conflicting order — not simply "WAL always before RAM".' : null);
  }
  function stepWhy(i) {
    return [
      'PostgreSQL must locate which heap page contains (or will contain) the target row.',
      'Pages are worked on in shared buffers (RAM); if the page isn\'t cached, it is read in first.',
      'The actual data change happens in memory, on the cached page.',
      'WAL captures what changed so it can be redone if the server crashes before the data page is durably written.',
      'This is the durability checkpoint: PostgreSQL must not acknowledge COMMIT until the transaction\'s WAL is safely durable.',
      'Once WAL is durable, it is safe to tell the client the transaction is committed.',
      'The modified page can stay dirty in RAM for a while; a background process or the next checkpoint will eventually write it out.',
    ][i];
  }
  return { render };
})();

/* ---------- 15 · Checkpoint ----------------------------------------------------- */

Sections.checkpoint = (() => {
  let pages;
  function fresh() { return [{ id: 10, dirty: false }, { id: 42, dirty: false }, { id: 77, dirty: false }]; }
  pages = fresh();
  function render() { pages = fresh(); draw(); document.querySelectorAll('[data-ckpt]').forEach(b => b.addEventListener('click', () => handle(b.dataset.ckpt))); }
  function draw() {
    document.getElementById('checkpointViz').innerHTML = `
      <div class="flow-tier">${pages.map(p => `<div class="flow-box ${p.dirty ? 'dirty' : 'clean'}">Page ${p.id}<div class="status-chip ${p.dirty ? 'no' : 'yes'}">${p.dirty ? 'DIRTY' : 'clean'}</div></div>`).join('')}</div>`;
  }
  function handle(action) {
    if (action === 'dirty') {
      pages.forEach(p => p.dirty = true);
      logEvent('Pages', 'Several pages modified in memory and marked dirty by ongoing transactions');
    }
    if (action === 'run') {
      const dirtyCount = pages.filter(p => p.dirty).length;
      pages.forEach(p => p.dirty = false);
      logEvent('Pages', `CHECKPOINT: ${dirtyCount} dirty page(s) flushed toward durable data files; a new recovery point is established`);
      explain('A checkpoint ran.', 'Checkpointing flushes dirty pages to disk and records a point recovery can start from, so it doesn\'t need to replay WAL from the very beginning.', 'A checkpoint\'s purpose is to bound recovery time — it establishes a recovery point and reduces how much WAL must be replayed after a crash. This simulation does not model PostgreSQL\'s actual checkpoint internals.');
    }
    if (action === 'reset') pages = fresh();
    draw();
  }
  return { render };
})();

/* ---------- 16 · Crash & Recovery ------------------------------------------------ */

Sections.recovery = (() => {
  function render() { draw(); document.querySelectorAll('[data-rec]').forEach(b => b.addEventListener('click', () => handle(b.dataset.rec))); }
  function draw() {
    const m = App.machine;
    document.getElementById('recoveryViz').innerHTML = m.crashed ? `
      <div class="flow-tier"><div class="flow-box dirty">RAM: LOST (server crashed)</div></div>
      <p class="pg-note">Durable state survives: data file has balance = ${m.page.diskValue}. WAL durable records: ${m.wal.durable.length}. Click "Run recovery" to replay WAL.</p>
    ` : `
      <div class="flow-tier">
        <div class="flow-box ${m.page.dirty ? 'dirty' : 'clean'}">Shared buffer<br>balance = ${m.page.bufferValue}${m.page.dirty ? '<div class="status-chip no">DIRTY</div>' : ''}</div>
        <div class="flow-box">Disk data file<br>balance = ${m.page.diskValue}</div>
        <div class="flow-box ${m.wal.durable.length ? 'durable' : 'pending'}">WAL<br>durable records: ${m.wal.durable.length}<br>buffered (not durable): ${m.wal.buffer.length}</div>
      </div>`;
  }
  function handle(action) {
    const m = App.machine;
    if (action === 'commit-durable') {
      m.page.bufferValue = 900; m.page.dirty = true;
      m.wal.durable.push({ lsn: '0/D1', tx: 201, text: 'balance 1000→900' });
      m.lastCommitDurable = true;
      logEvent('WAL', 'UPDATE + WAL flush + COMMIT: WAL is durable, page is still dirty in RAM only');
      explain('A durable commit happened.', 'The WAL record for this change reached durable storage before commit was acknowledged — the data page itself is still only dirty in RAM.', 'This is exactly why durability is defined in terms of WAL, not the data page: the page can lag behind safely.');
    }
    if (action === 'commit-not-durable') {
      m.page.bufferValue = 700; m.page.dirty = true;
      m.wal.buffer.push({ lsn: '0/D2', tx: 202, text: 'balance 900→700' });
      m.lastCommitDurable = false;
      logEvent('WAL', 'UPDATE applied in memory, but WAL was NOT flushed before crash — this change is still in-flight/unacknowledged');
      explain('An in-flight, non-durable change was made.', 'Because its WAL record never reached durable storage, this change has no durability guarantee yet.', 'A transaction whose WAL never became durable did not survive to be a "committed" transaction from recovery\'s point of view.');
    }
    if (action === 'crash') {
      m.crashed = true;
      m.page.dirty = false; // RAM contents lost, buffer state no longer meaningful
      logEvent('Recovery', '💥 CRASH — all RAM state (shared buffers, WAL buffer) is lost; durable WAL and durable data files survive');
      explain('The database crashed.', 'Everything in RAM is gone. Only what was made durable — the WAL segments already flushed, and the data file as it was last written — survives.', 'This is exactly the boundary durability is defined against: what survived a crash is, by definition, what was durable.');
    }
    if (action === 'recover') {
      if (!m.crashed) return;
      let replayed = 0;
      m.wal.durable.forEach(rec => { m.page.diskValue = parseInt(rec.text.split('→')[1]); replayed++; });
      m.crashed = false;
      m.page.bufferValue = m.page.diskValue; m.page.dirty = false;
      logEvent('Recovery', `Recovery: replayed ${replayed} durable WAL record(s). Recovered balance = ${m.page.diskValue}. ${m.lastCommitDurable === false ? 'The non-durable change was NOT replayed — it never actually committed durably.' : ''}`);
      explain('Recovery completed.', 'The recovery process reads durable WAL and replays the records needed to reconstruct all committed changes onto the data files.', m.lastCommitDurable === false
        ? 'An unacknowledged transaction whose WAL never became durable does not survive a crash — that is expected and correct, not data loss of a committed transaction.'
        : 'A committed, durable transaction always survives a crash because recovery replays its WAL.');
    }
    if (action === 'reset') {
      m.page = { id: 42, diskValue: 1000, bufferValue: 1000, dirty: false, cached: true };
      m.wal = { buffer: [], durable: [], nextLsn: 1 };
      m.crashed = false; m.lastCommitDurable = null;
    }
    draw(); renderMemDiskDock();
  }
  return { render };
})();

/* ---------- 17 · VACUUM ---------------------------------------------------------- */

Sections.vacuum = (() => {
  let tuples;
  function fresh() { return [
    { id: 1, status: 'live' }, { id: 2, status: 'live' }, { id: 3, status: 'live' },
    { id: 4, status: 'live' }, { id: 5, status: 'live' }, { id: 6, status: 'live' },
  ]; }
  tuples = fresh();
  function render() { tuples = fresh(); draw(); document.querySelectorAll('[data-vac]').forEach(b => b.addEventListener('click', () => handle(b.dataset.vac))); }
  function draw() {
    document.getElementById('heapPageViz').innerHTML = tuples.map(t =>
      `<div class="tuple-cell ${t.status}">#${t.id}<br>${t.status}</div>`).join('');
  }
  function handle(action) {
    if (action === 'update' || action === 'delete') {
      const liveTuple = tuples.find(t => t.status === 'live');
      if (!liveTuple) return;
      liveTuple.status = 'dead';
      if (action === 'update') {
        const newId = Math.max(...tuples.map(t => t.id)) + 1;
        tuples.push({ id: newId, status: 'live' });
        logEvent('MVCC', `UPDATE creates a new tuple version (#${newId}); tuple #${liveTuple.id} becomes a dead (obsolete) version`);
      } else {
        logEvent('MVCC', `DELETE marks tuple #${liveTuple.id} as dead once no active transaction still needs to see it`);
      }
      explain('A dead tuple was created.', 'MVCC keeps old row versions around so concurrent transactions with older snapshots can still see them; once nothing needs a version, it becomes reclaimable.', 'PostgreSQL MVCC produces dead tuples as a normal side effect of UPDATE/DELETE — they are not a bug, but they do need periodic cleanup.');
    }
    if (action === 'run') {
      const before = tuples.filter(t => t.status === 'dead').length;
      tuples.forEach(t => { if (t.status === 'dead') t.status = 'reclaimed'; });
      logEvent('VACUUM', `VACUUM reclaimed ${before} dead tuple(s) — their space is now reusable for future inserts/updates on this table`);
      explain('VACUUM ran.', 'VACUUM scans the table, identifies dead tuples no longer visible to any transaction, and marks their space reusable.', 'Ordinary VACUUM does not typically shrink the physical table file — it makes space available for reuse inside the existing file.');
    }
    if (action === 'full') {
      const beforeLen = tuples.length;
      tuples = tuples.filter(t => t.status !== 'reclaimed' && t.status !== 'dead');
      logEvent('VACUUM', `VACUUM FULL rewrote the table into a new, compact file — table shrank from ${beforeLen} to ${tuples.length} tuple slots (takes a strong lock while running)`);
      explain('VACUUM FULL ran.', 'Unlike ordinary VACUUM, VACUUM FULL rewrites the entire table into a new file with no dead space, then swaps it in.', 'VACUUM FULL reclaims disk space back to the OS, but requires an exclusive lock on the table for its duration — it is not something you run casually on a live table.');
    }
    if (action === 'reset') tuples = fresh();
    draw();
  }
  return { render };
})();

/* ---------- 18 · Complete Flow --------------------------------------------------- */

Sections.fullflow = (() => {
  const steps = [
    'BEGIN', 'Read page', 'Page enters shared buffers', 'UPDATE',
    'New MVCC version conceptually created', 'Old version becomes obsolete once no longer needed',
    'Page marked DIRTY', 'WAL record generated', 'WAL buffered', 'WAL flushed/durable',
    'COMMIT', 'Commit acknowledged', 'Dirty page remains in memory',
    'CHECKPOINT', 'Dirty page written to disk', 'VACUUM later cleans eligible dead tuples',
  ];
  let idx, running;
  function render() { idx = -1; running = false; draw(); wire(); }
  function wire() {
    document.querySelectorAll('[data-flow]').forEach(b => b.addEventListener('click', () => handle(b.dataset.flow)));
    document.querySelectorAll('#fullFlowTimeline li').forEach((li, i) => li.addEventListener('click', () => { idx = i; running = false; draw(); logEvent('Transactions', `Jumped to step: ${steps[i]}`); }));
  }
  function draw() {
    document.getElementById('fullFlowTimeline').innerHTML = steps.map((s, i) =>
      `<li class="${i < idx ? 'done' : i === idx ? 'current' : ''}">${s}</li>`).join('');
    wire();
  }
  async function handle(action) {
    if (action === 'reset') { idx = -1; running = false; draw(); return; }
    if (action === 'run') {
      running = true;
      App.playing = true; updatePlayButtons();
      while (running && idx < steps.length - 1) {
        idx++;
        draw();
        logEvent('Transactions', `Complete flow — step ${idx + 1}: ${steps[idx]}`);
        explain(steps[idx], 'Part of the single, continuous path a real UPDATE...COMMIT takes through memory, WAL, and disk.', 'Being able to narrate this whole path — UPDATE → MVCC → dirty page → WAL → commit → checkpoint → VACUUM — in order is one of the best signals of real understanding in an interview.');
        await wait(App.speedMs);
        if (!App.playing) { running = false; break; }
      }
      App.playing = false; updatePlayButtons();
    }
  }
  return { render, step: () => handle('run') };
})();

/* ---------- 19 · Interview Mode --------------------------------------------------- */

const InterviewBank = [
  { q: 'What is a transaction?', kw: ['unit', 'work', 'atomic', 'all', 'none', 'commit', 'rollback'], ref: 'A transaction is a unit of work, made up of one or more operations, that the database applies either completely or not at all.' },
  { q: 'Explain ACID.', kw: ['atomicity', 'consistency', 'isolation', 'durability', 'atomic', 'invariant', 'concurrent', 'crash'], ref: 'Atomicity: all-or-nothing. Consistency: valid-state-to-valid-state, respecting constraints. Isolation: concurrent transactions don\'t improperly see each other\'s in-progress work. Durability: once committed, changes survive a crash.' },
  { q: 'What is a dirty read?', kw: ['uncommitted', 'read', 'another', 'transaction', 'rollback'], ref: 'Reading data written by another transaction that has not yet committed — and might later roll back.' },
  { q: 'What is a non-repeatable read?', kw: ['reread', 'read', 'again', 'different', 'value', 'committed', 'change'], ref: 'Re-reading the same row within one transaction and getting a different value because another transaction committed a change in between.' },
  { q: 'What is a phantom read?', kw: ['range', 'query', 'rows', 'insert', 'appear', 'new', 'row', 'set'], ref: 'Re-running the same range query within one transaction and getting a different set of matching rows, because another transaction inserted or deleted a matching row and committed.' },
  { q: 'What are isolation levels?', kw: ['read', 'uncommitted', 'committed', 'repeatable', 'serializable', 'anomalies', 'concurrency'], ref: 'A spectrum of guarantees (Read Uncommitted, Read Committed, Repeatable Read, Serializable) that trade off how many concurrency anomalies are prevented against how much concurrency is allowed.' },
  { q: 'What is MVCC?', kw: ['multi', 'version', 'concurrency', 'control', 'snapshot', 'row', 'versions', 'readers', 'writers', 'block'], ref: 'Multi-Version Concurrency Control: the database keeps multiple versions of a row so readers can see a consistent snapshot without blocking writers, and vice versa.' },
  { q: 'Why does PostgreSQL use MVCC?', kw: ['readers', 'writers', 'block', 'concurrency', 'snapshot', 'lock'], ref: 'So readers and writers don\'t block each other — a reader sees a consistent snapshot of committed data while writers create new versions, instead of everyone contending for locks.' },
  { q: 'What is a dirty page?', kw: ['memory', 'modified', 'differ', 'disk', 'durable', 'not', 'written'], ref: 'A page in memory (shared buffers) that has been modified and whose contents differ from the durable copy in the data file — not a corrupted page.' },
  { q: 'What are shared buffers?', kw: ['ram', 'memory', 'cache', 'pages', 'disk', 'hit', 'miss'], ref: 'PostgreSQL\'s in-memory cache of data pages, sitting between the query engine and the disk, so frequently used pages don\'t need to be re-read from storage every time.' },
  { q: 'What is WAL?', kw: ['write', 'ahead', 'log', 'record', 'redo', 'recovery', 'durable', 'crash'], ref: 'Write-Ahead Log: a durable, sequential record of the changes needed to redo transactions during crash recovery, written before the corresponding change is durably applied to the data file.' },
  { q: 'Why does PostgreSQL need WAL?', kw: ['durability', 'crash', 'recovery', 'redo', 'without', 'flushing', 'every', 'page'], ref: 'So it can guarantee durability without having to flush every modified data page to disk on every commit — flushing a small sequential WAL record is far cheaper.' },
  { q: 'Does COMMIT mean the data page is already on disk?', kw: ['no', 'wal', 'durable', 'dirty', 'later', 'not necessarily'], ref: 'No. COMMIT means the transaction\'s WAL is durable. The modified data page can remain dirty in memory and be written out later, e.g. at checkpoint.' },
  { q: 'What happens during crash recovery?', kw: ['replay', 'wal', 'durable', 'redo', 'committed', 'reconstruct'], ref: 'The database reads the durable WAL and replays the records needed to reconstruct all committed changes on the data files, bringing the database back to a consistent state.' },
  { q: 'What is VACUUM?', kw: ['dead', 'tuple', 'reclaim', 'space', 'reusable', 'bloat', 'mvcc'], ref: 'A maintenance process that identifies dead tuple versions left behind by MVCC and marks their space reusable, helping control table bloat.' },
  { q: 'Difference between optimistic and pessimistic locking?', kw: ['assume', 'rare', 'likely', 'lock', 'upfront', 'version', 'conflict', 'wait'], ref: 'Optimistic assumes conflicts are rare, takes no lock upfront, and detects conflicts at write time (often via a version column). Pessimistic assumes conflicts are likely and acquires a lock before the critical operation, making others wait.' },
  { q: 'What is a deadlock?', kw: ['circular', 'wait', 'each other', 'abort', 'detect'], ref: 'A circular wait between two or more transactions, each waiting on a resource the other holds, which cannot resolve on its own — the database detects it and aborts one transaction.' },
  { q: 'Lock wait vs deadlock?', kw: ['normal', 'resolve', 'own', 'commit', 'circular', 'cannot'], ref: 'A lock wait resolves by itself once the lock holder commits or rolls back. A deadlock is a circular wait that cannot resolve on its own and requires the database to abort a participant.' },
  { q: 'What happens when an UPDATE occurs internally at a high level?', kw: ['page', 'buffer', 'modify', 'wal', 'dirty', 'version'], ref: 'The relevant page is loaded into shared buffers if needed, modified in memory (conceptually creating a new MVCC version), marked dirty, and a WAL record describing the change is generated.' },
  { q: 'Explain the complete path from UPDATE to WAL to COMMIT to checkpoint to recovery.', kw: ['update', 'wal', 'commit', 'checkpoint', 'recovery', 'dirty', 'durable', 'replay'], ref: 'UPDATE modifies a page in shared buffers and marks it dirty; a WAL record is generated and flushed durably; COMMIT is acknowledged once WAL is durable; the dirty page may still be flushed later, e.g. at a checkpoint; if a crash happens before that flush, recovery replays the durable WAL to reconstruct the committed change.' },
];

Sections.interview = (() => {
  let order, pos;
  function fresh() { order = InterviewBank.map((_, i) => i); pos = 0; }
  fresh();

  function render() {
    fresh();
    draw();
    document.querySelectorAll('[data-iv]').forEach(b => b.addEventListener('click', () => handle(b.dataset.iv)));
  }
  function draw() {
    document.getElementById('interviewProgress').textContent = `Question ${pos + 1} of ${order.length}`;
    document.getElementById('interviewQuestion').textContent = InterviewBank[order[pos]].q;
    document.getElementById('interviewAnswer').value = '';
    document.getElementById('interviewResult').innerHTML = '';
  }
  function grade(answer, item) {
    const a = answer.toLowerCase();
    const hits = item.kw.filter(k => a.includes(k)).length;
    const ratio = hits / item.kw.length;
    if (a.trim().length < 3) return 'incorrect';
    if (ratio >= 0.5) return 'correct';
    if (ratio >= 0.2) return 'partial';
    return 'incorrect';
  }
  function handle(action) {
    if (action === 'next') { pos = (pos + 1) % order.length; draw(); return; }
    if (action === 'skip') { pos = (pos + 1) % order.length; draw(); return; }
    if (action === 'check') {
      const item = InterviewBank[order[pos]];
      const answer = document.getElementById('interviewAnswer').value;
      const verdict = grade(answer, item);
      const label = { correct: 'Correct', partial: 'Partially correct', incorrect: 'Incorrect' }[verdict];
      document.getElementById('interviewResult').innerHTML = `
        <span class="iv-verdict ${verdict}">${label}</span>
        <p><strong>Reference answer:</strong> ${item.ref}</p>`;
      logEvent('Transactions', `Interview question graded: "${item.q}" → ${label}`);
      explain(`Answer graded: ${label}.`, 'Grading here is a simple keyword-overlap heuristic against the reference answer, not a full semantic check — use it as a self-check prompt, not a certification.', item.ref, 'Interview-mode grading is a simplified heuristic, not an authoritative assessment.');
    }
  }
  return { render };
})();

/* ---------------------------------------------------------------------- *
 * 4. GLOBAL WIRING
 * ---------------------------------------------------------------------- */

function updatePlayButtons() {
  document.getElementById('btnPlay').disabled = App.playing;
  document.getElementById('btnPause').disabled = !App.playing;
}

function resetEverything() {
  App.playing = false;
  App.accounts = { A: 1000, B: 1000 };
  App.machine = {
    page: { id: 42, diskValue: 1000, bufferValue: 1000, dirty: false, cached: true },
    wal: { buffer: [], durable: [], nextLsn: 1 },
    bufferStats: { hits: 0, misses: 0 },
    lastCommitDurable: null,
    crashed: false,
  };
  App.eventLog = [];
  logEvent('Transactions', 'Simulation reset — all sections returned to their initial state');
  goToSection(App.currentSection);
  refreshDock();
  explain('Simulation reset.', 'All shared state (accounts, pages, WAL, locks) returned to its initial values.', 'Resetting gives you a clean slate to re-run a scenario from scratch.');
}

function wireGlobalControls() {
  document.getElementById('navList').addEventListener('click', e => {
    const li = e.target.closest('li[data-section]');
    if (li) goToSection(li.dataset.section);
  });

  document.getElementById('modeGeneric').addEventListener('click', () => setMode('generic'));
  document.getElementById('modePostgres').addEventListener('click', () => setMode('postgres'));

  function setMode(m) {
    App.mode = m;
    document.getElementById('modeGeneric').classList.toggle('active', m === 'generic');
    document.getElementById('modePostgres').classList.toggle('active', m === 'postgres');
    logEvent('Transactions', `Learning mode switched to ${m === 'postgres' ? 'PostgreSQL' : 'Generic Database'}`);
    goToSection(App.currentSection); // re-render current section with new mode
  }

  document.getElementById('isolationLevel').addEventListener('change', e => {
    App.isolation = e.target.value;
    logEvent('Transactions', `Isolation level set to ${labelIso(App.isolation)}`);
    goToSection(App.currentSection);
  });

  document.getElementById('speedSelect').addEventListener('change', e => {
    App.speedMs = parseInt(e.target.value, 10);
  });

  document.getElementById('btnPlay').addEventListener('click', () => {
    if (App.currentSection === 'fullflow' && Sections.fullflow.step) {
      Sections.fullflow.step();
    } else if (App.currentSection === 'walcommit') {
      document.querySelector('[data-wc="run-all"]').click();
    } else {
      explain('Nothing to play here.', 'This section doesn\'t have a continuous animated sequence — use its own buttons, or visit "Complete Flow".', 'Play is wired to the Complete Flow and WAL+Commit sequences.');
    }
  });
  document.getElementById('btnPause').addEventListener('click', () => {
    App.playing = false;
    updatePlayButtons();
  });

  document.getElementById('btnStep').addEventListener('click', () => {
    if (App.currentSection === 'fullflow') document.querySelector('[data-flow="run"]')?.click();
    else if (App.currentSection === 'walcommit') document.querySelector('[data-wc="run-step"]')?.click();
    else if (App.currentSection === 'deadlocks') document.querySelector('[data-dead="run"]')?.click();
    else explain('No single "step" defined for this section.', 'Use the section\'s own buttons to drive its state one action at a time.', 'Most sections here are driven by explicit SQL-like actions rather than a generic step function, which mirrors how you\'d actually issue commands.');
  });

  document.getElementById('btnReset').addEventListener('click', resetEverything);
  document.getElementById('btnClearLog').addEventListener('click', () => { App.eventLog = []; renderLog(); renderTimelineDock(); });

  document.getElementById('btnCrash').addEventListener('click', () => {
    goToSection('recovery');
    setTimeout(() => document.querySelector('[data-rec="crash"]').click(), 50);
  });

  document.getElementById('dockTabs').addEventListener('click', e => {
    const btn = e.target.closest('.dock-tab');
    if (!btn) return;
    document.querySelectorAll('.dock-tab').forEach(t => t.classList.toggle('active', t === btn));
    document.querySelectorAll('.dock-panel').forEach(p => p.classList.toggle('active', p.dataset.panel === btn.dataset.tab));
  });

  document.getElementById('logFilters').addEventListener('click', e => {
    const chip = e.target.closest('.filter-chip');
    if (!chip) return;
    App.logFilter = chip.dataset.filter;
    document.querySelectorAll('.filter-chip').forEach(c => c.classList.toggle('active', c === chip));
    renderLog();
  });
}

/* ---------------------------------------------------------------------- *
 * BOOTSTRAP
 * ---------------------------------------------------------------------- */

document.addEventListener('DOMContentLoaded', () => {
  window.ConcurrencyModule = Sections.concurrency;
  wireGlobalControls();
  goToSection('basics');
  logEvent('Transactions', 'Simulator loaded. Start with Transaction Basics, or pick any section from the sidebar.');
  refreshDock();
});
