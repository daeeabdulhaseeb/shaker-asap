/**
 * Shaker Dashboard Auth Module
 * Include this in every dashboard page.
 * 
 * Usage:
 *   ShakerAuth.init({ onLogin: function(session){...}, onFail: function(){...} })
 *   ShakerAuth.getSession()   -> { token, role, team } or null
 *   ShakerAuth.logout()
 *   ShakerAuth.isAdmin()      -> true if role === 'admin'
 *   ShakerAuth.getTeam()      -> team string or null (null = all teams)
 *   ShakerAuth.canSee(tab)    -> true if current role can see this tab
 *                                tabs: 'all','redlist','evp','inter','openbox'
 */

var ShakerAuth = (function() {

  var WORKER = 'https://shaker-asap-auth.daeeabdulhaseeb.workers.dev';
  var SESSION_KEY = 'shaker_session';

  var ROLES = [
    { value: 'evp',              label: 'EVP' },
    { value: 'admin',            label: 'Admin / Ops' },
    { value: 'manager_central',  label: 'Central Projects Manager' },
    { value: 'manager_acscr',    label: 'ACS - CR Manager' },
    { value: 'manager_west',     label: 'West Projects Manager' },
    { value: 'manager_acswr',    label: 'ACS - WR Manager' },
    { value: 'manager_east',     label: 'East Projects Manager' },
    { value: 'manager_acser',    label: 'ACS - ER Manager' },
    { value: 'manager_direct',   label: 'Direct Channel Manager' },
  ];

  var FULL_ACCESS = ['admin', 'evp'];

  var TAB_ACCESS = {
    redlist: ['admin', 'evp', 'manager_central', 'manager_acscr', 'manager_west',
              'manager_acswr', 'manager_east', 'manager_acser', 'manager_direct'],
    evp:     ['admin', 'evp'],
    inter:   ['admin', 'evp'],
    openbox: ['admin', 'evp', 'manager_central', 'manager_acscr', 'manager_west',
              'manager_acswr', 'manager_east', 'manager_acser', 'manager_direct'],
  };

  var _session = null;
  var _onLogin = null;
  var _onFail  = null;

  function _loadSession() {
    try {
      var raw = sessionStorage.getItem(SESSION_KEY);
      if (raw) _session = JSON.parse(raw);
    } catch(e) { _session = null; }
  }

  function _saveSession(s) {
    _session = s;
    sessionStorage.setItem(SESSION_KEY, JSON.stringify(s));
  }

  function _clearSession() {
    _session = null;
    sessionStorage.removeItem(SESSION_KEY);
  }

  function _renderLoginUI() {
    var overlay = document.createElement('div');
    overlay.id = 'shaker-auth-overlay';
    overlay.style.cssText = [
      'position:fixed;top:0;left:0;width:100vw;height:100vh',
      'background:#f8f9fa;z-index:9999',
      'display:flex;align-items:center;justify-content:center',
      'font-family:"DM Sans",sans-serif'
    ].join(';');

    overlay.innerHTML = [
      '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:40px;width:360px;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.08)">',
        '<div style="font-size:13px;font-weight:500;color:#6b7280;margin-bottom:2px">Shaker Group</div>',
        '<div style="font-size:20px;font-weight:500;color:#111827;margin-bottom:4px">ASAP Sales Dashboard</div>',
        '<div style="font-size:11px;color:#9ca3af;margin-bottom:24px">Sign in to continue</div>',
        '<div style="text-align:left;margin-bottom:12px">',
          '<label style="font-size:11px;font-weight:500;color:#374151;display:block;margin-bottom:4px">Your role</label>',
          '<select id="auth-role-sel" style="width:100%;padding:8px 10px;border:1px solid #d1d5db;border-radius:8px;font-size:12px;color:#111827;background:#fff;height:36px">',
            ROLES.map(function(r){ return '<option value="'+r.value+'">'+r.label+'</option>'; }).join(''),
          '</select>',
        '</div>',
        '<div style="text-align:left;margin-bottom:16px">',
          '<label style="font-size:11px;font-weight:500;color:#374151;display:block;margin-bottom:4px">PIN</label>',
          '<div style="display:flex;gap:8px;justify-content:center">',
            '<input type="password" maxlength="1" id="auth-p0" style="width:52px;height:52px;border:1px solid #d1d5db;border-radius:8px;font-size:22px;font-weight:500;text-align:center;background:#fff;color:#111827">',
            '<input type="password" maxlength="1" id="auth-p1" style="width:52px;height:52px;border:1px solid #d1d5db;border-radius:8px;font-size:22px;font-weight:500;text-align:center;background:#fff;color:#111827">',
            '<input type="password" maxlength="1" id="auth-p2" style="width:52px;height:52px;border:1px solid #d1d5db;border-radius:8px;font-size:22px;font-weight:500;text-align:center;background:#fff;color:#111827">',
            '<input type="password" maxlength="1" id="auth-p3" style="width:52px;height:52px;border:1px solid #d1d5db;border-radius:8px;font-size:22px;font-weight:500;text-align:center;background:#fff;color:#111827">',
          '</div>',
        '</div>',
        '<button id="auth-submit" style="width:100%;padding:10px;background:#1d4ed8;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:500;margin-bottom:8px">Sign in</button>',
        '<div id="auth-error" style="font-size:11px;color:#dc2626;min-height:16px"></div>',
        '<div style="font-size:10px;color:#9ca3af;margin-top:12px">Secured by Cloudflare &nbsp;&middot;&nbsp; Session expires after 8 hours</div>',
      '</div>'
    ].join('');

    document.body.appendChild(overlay);

    // PIN digit navigation
    [0,1,2,3].forEach(function(i) {
      var el = document.getElementById('auth-p'+i);
      el.addEventListener('input', function() {
        if (el.value && i < 3) document.getElementById('auth-p'+(i+1)).focus();
        if (i === 3 && el.value) _submitLogin();
      });
      el.addEventListener('keydown', function(e) {
        if (e.key === 'Backspace' && !el.value && i > 0)
          document.getElementById('auth-p'+(i-1)).focus();
      });
    });

    document.getElementById('auth-submit').addEventListener('click', _submitLogin);

    // Focus first PIN digit
    setTimeout(function(){ document.getElementById('auth-p0').focus(); }, 100);
  }

  function _getPin() {
    return [0,1,2,3].map(function(i){
      return (document.getElementById('auth-p'+i)||{value:''}).value;
    }).join('');
  }

  function _clearPin() {
    [0,1,2,3].forEach(function(i){
      var el = document.getElementById('auth-p'+i);
      if (el) el.value = '';
    });
    var f = document.getElementById('auth-p0');
    if (f) f.focus();
  }

  function _setError(msg) {
    var el = document.getElementById('auth-error');
    if (el) el.textContent = msg;
  }

  function _setSubmitting(busy) {
    var btn = document.getElementById('auth-submit');
    if (!btn) return;
    btn.disabled = busy;
    btn.textContent = busy ? 'Signing in...' : 'Sign in';
  }

  function _submitLogin() {
    var role = document.getElementById('auth-role-sel').value;
    var pin = _getPin();
    if (pin.length < 4) { _setError('Enter your 4-digit PIN'); return; }
    _setError('');
    _setSubmitting(true);

    fetch(WORKER + '/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: role, pin: pin })
    })
    .then(function(r){ return r.json(); })
    .then(function(data) {
      _setSubmitting(false);
      if (data.error) {
        _setError(data.error === 'Incorrect PIN' ? 'Incorrect PIN. Try again.' : data.error);
        _clearPin();
        return;
      }
      _saveSession({ token: data.token, role: data.role, team: data.team, expires: data.expires });
      var overlay = document.getElementById('shaker-auth-overlay');
      if (data.role === 'evp' && overlay) {
        overlay.innerHTML = '<div style="text-align:center;font-family:\'DM Sans\',sans-serif">'
          + '<div style="font-size:28px;margin-bottom:12px">&#128075;</div>'
          + '<div style="font-size:22px;font-weight:500;color:#111827;margin-bottom:6px">Welcome, Mr. Eddie</div>'
          + '<div style="font-size:13px;color:#6b7280">Loading your dashboard...</div>'
          + '</div>';
        setTimeout(function() {
          if (overlay) overlay.remove();
          if (_onLogin) _onLogin(_session);
        }, 1800);
      } else {
        if (overlay) overlay.remove();
        if (_onLogin) _onLogin(_session);
      }
    })
    .catch(function(err) {
      _setSubmitting(false);
      _setError('Connection error. Try again.');
      console.error('Auth error:', err);
    });
  }

  function _verifySession(token, callback) {
    fetch(WORKER + '/verify?token=' + encodeURIComponent(token))
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.valid) callback(true);
      else { _clearSession(); callback(false); }
    })
    .catch(function() { callback(false); });
  }

  // ── Public API ─────────────────────────────────────────────

  function init(opts) {
    _onLogin = opts.onLogin || null;
    _onFail  = opts.onFail  || null;
    _loadSession();

    if (_session && _session.token) {
      // Verify session is still valid with Worker
      _verifySession(_session.token, function(valid) {
        if (valid) {
          if (_onLogin) _onLogin(_session);
        } else {
          _renderLoginUI();
        }
      });
    } else {
      _renderLoginUI();
    }
  }

  function getSession() { return _session; }

  function isAdmin() {
    return _session && (_session.role === 'admin' || _session.role === 'evp');
  }

  function getTeam() {
    if (!_session) return null;
    if (FULL_ACCESS.indexOf(_session.role) >= 0) return null; // null = all teams
    return _session.team || null;
  }

  function getRole() {
    return _session ? _session.role : null;
  }

  function canSee(tab) {
    if (!_session) return false;
    var allowed = TAB_ACCESS[tab];
    if (!allowed) return true; // unknown tab = open
    return allowed.indexOf(_session.role) >= 0;
  }

  function logout() {
    if (_session && _session.token) {
      fetch(WORKER + '/logout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: _session.token })
      }).catch(function(){});
    }
    _clearSession();
    location.reload();
  }

  // ── Claims API ─────────────────────────────────────────────

  function loadClaims(callback) {
    if (!_session) { callback({}); return; }
    fetch(WORKER + '/claims?token=' + encodeURIComponent(_session.token))
    .then(function(r){ return r.json(); })
    .then(function(data){ callback(data.claims || {}); })
    .catch(function(){ callback({}); });
  }

  function saveClaim(custNo, claimedBy, claimedDate, claimedTeam, callback) {
    if (!_session || !isAdmin()) {
      if (callback) callback({ error: 'Not authorized' });
      return;
    }
    fetch(WORKER + '/claim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        token: _session.token,
        cust_no: custNo,
        claimed_by: claimedBy,
        claimed_date: claimedDate,
        claimed_team: claimedTeam
      })
    })
    .then(function(r){ return r.json(); })
    .then(function(data){ if (callback) callback(data); })
    .catch(function(err){ if (callback) callback({ error: err.message }); });
  }

  function deleteClaim(custNo, callback) {
    if (!_session || !isAdmin()) {
      if (callback) callback({ error: 'Not authorized' });
      return;
    }
    fetch(WORKER + '/claim', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: _session.token, cust_no: custNo })
    })
    .then(function(r){ return r.json(); })
    .then(function(data){ if (callback) callback(data); })
    .catch(function(err){ if (callback) callback({ error: err.message }); });
  }

  return {
    init:        init,
    getSession:  getSession,
    getRole:     getRole,
    getTeam:     getTeam,
    isAdmin:     isAdmin,
    canSee:      canSee,
    logout:      logout,
    loadClaims:  loadClaims,
    saveClaim:   saveClaim,
    deleteClaim: deleteClaim,
  };

})();
