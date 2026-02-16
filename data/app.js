// Global variables: 4 sensors × (temp + hum) = 8 charts
let tempCharts = [];
let humCharts = [];
let deviceConfig = {};
let autoRefreshInterval = null;
let statusRefreshInterval = null;

let ws = null;
let wsConnected = false;

function setWsBadge(text, ok) {
    const el = document.getElementById('wsStatus');
    if (!el) return;
    el.textContent = 'WS: ' + text;
    el.className = ok ? 'status-badge status-ok' : 'status-badge status-warning';
}

function wsConnect() {
    try {
        const url = `ws://${location.hostname}:81/`;
        ws = new WebSocket(url);
        setWsBadge('Connecting', false);

        ws.onopen = () => {
            wsConnected = true;
            setWsBadge('OK', true);
            console.log('[WS] connected');
        };

        ws.onclose = () => {
            wsConnected = false;
            setWsBadge('Closed', false);
            console.log('[WS] closed, retrying');
            // retry
            setTimeout(wsConnect, 1000);
        };

        ws.onerror = () => {
            wsConnected = false;
            setWsBadge('Error', false);
            console.log('[WS] error');
        };

        ws.onmessage = (ev) => {
            try {
                // console.log('[WS] msg', ev.data);
                const msg = JSON.parse(ev.data);
                if (!msg || !msg.type) return;
                if (msg.type === 'config') {
                    console.log('[WS] config', msg);
                    // update local config snapshot
                    if (Array.isArray(msg.sample_period_sensor)) deviceConfig.sample_period_sensor = msg.sample_period_sensor;
                    if (Array.isArray(msg.heating_duration_sensor)) deviceConfig.heating_duration_sensor = msg.heating_duration_sensor;
                    if (Array.isArray(msg.heating_interval_sensor)) deviceConfig.heating_interval_sensor = msg.heating_interval_sensor;
                    if (deviceConfig.sample_period_sensor) deviceConfig.sample_period_s = deviceConfig.sample_period_sensor[0];
                    updateAllSensorRowSettings();
                } else if (msg.type === 'sample') {
                    // console.log('[WS] sample', msg);
                    // fast chart append
                    const s = Number(msg.sensor);
                    const ts = new Date(msg.ts).getTime();
                    const t = Number(msg.t);
                    const h = Number(msg.h);
                    if (Number.isFinite(s) && s >= 0 && s < 4 && Number.isFinite(ts)) {
                        const MAX_LIVE_POINTS = 2000; // avoid unbounded growth
                        if (Number.isFinite(t) && tempCharts[s]) {
                            const cur = tempCharts[s].w.config.series[0].data || [];
                            cur.push([ts, t]);
                            if (cur.length > MAX_LIVE_POINTS) cur.splice(0, cur.length - MAX_LIVE_POINTS);
                            tempCharts[s].updateSeries([{ name: 'Temperature', data: cur }], false);
                        }
                        if (Number.isFinite(h) && humCharts[s]) {
                            const cur = humCharts[s].w.config.series[0].data || [];
                            cur.push([ts, h]);
                            if (cur.length > MAX_LIVE_POINTS) cur.splice(0, cur.length - MAX_LIVE_POINTS);
                            humCharts[s].updateSeries([{ name: 'Humidity', data: cur }], false);
                        }
                    }
                }
            } catch (e) {
                // ignore
            }
        };
    } catch (e) {
        setWsBadge('Unavailable', false);
    }
}

function wsSend(obj) {
    if (!wsConnected || !ws) return false;
    try {
        ws.send(JSON.stringify(obj));
        return true;
    } catch (e) {
        return false;
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing FireBeetle 2 SHT85 Logger...');
    initializeCharts();
    loadConfig();
    loadStorageStatus();
    checkTimeStatus();
    wsConnect();
    
    // Set default time range and auto-load data (7 days to catch device time drift)
    setQuickRange(7, true);
    
    // Start live status updates (every 5 seconds)
    updateLiveStatus();
    statusRefreshInterval = setInterval(function() {
        updateLiveStatus();
    }, 5000);
    console.log('Live status updates started (5s interval)');
    
    // Start auto-refresh for charts (every 30 seconds)
    startAutoRefresh(30);
    console.log('Auto-refresh started (30s interval)');
});

// Live status update
async function updateLiveStatus() {
    try {
        const response = await fetch('/api/status');
        if (response.ok) {
            const data = await response.json();
            
            const sensors = data.sensors || [];
            const heaterOnArr = (data.heating && Array.isArray(data.heating.on_sensor)) ? data.heating.on_sensor : [];
            for (let i = 0; i < 4; i++) {
                const liveEl = document.getElementById('liveStatus' + i);
                if (!liveEl) continue;
                const s = sensors[i];
                const heaterOn = heaterOnArr[i] === true;
                if (s && s.connected && typeof s.temperature === 'number' && typeof s.humidity === 'number' && !Number.isNaN(s.temperature)) {
                    liveEl.textContent = 'S' + i + ': ' + s.temperature.toFixed(1) + '°C ' + s.humidity.toFixed(0) + '%' + (heaterOn ? ' (heater on)' : '');
                    liveEl.className = 'status-badge live-sensor status-ok';
                } else {
                    liveEl.textContent = 'S' + i + ': --';
                    liveEl.className = 'status-badge live-sensor status-warning';
                }
            }
            
            // Update time display
            const timeEl = document.getElementById('timeStatus');
            if (timeEl && data.time.set) {
                // Convert UTC to local time
                const deviceTime = new Date(data.time.iso);
                const localTime = deviceTime.toLocaleTimeString('cs-CZ', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                timeEl.textContent = localTime;
                timeEl.className = 'status-badge status-ok';
            }
        }
    } catch (error) {
        console.error('Failed to update live status:', error);
        for (let i = 0; i < 4; i++) {
            const liveEl = document.getElementById('liveStatus' + i);
            if (liveEl) {
                liveEl.textContent = 'S' + i + ': Offline';
                liveEl.className = 'status-badge live-sensor status-error';
            }
        }
    }
}

// Auto-refresh control
function startAutoRefresh(seconds) {
    stopAutoRefresh();
    if (seconds > 0) {
        autoRefreshInterval = setInterval(function() {
            console.log('Auto-refreshing data...');
            // Update time range to include new data
            document.getElementById('toDate').value = formatDateTimeLocal(new Date());
            loadData(true); // silent refresh
        }, seconds * 1000);
        
        const btn = document.getElementById('autoRefreshBtn');
        if (btn) {
            btn.textContent = `Auto: ${seconds}s`;
            btn.classList.add('btn-active');
        }
        console.log('Auto-refresh enabled: ' + seconds + 's');
    }
}

function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
        console.log('Auto-refresh disabled');
    }
    const btn = document.getElementById('autoRefreshBtn');
    if (btn) {
        btn.textContent = 'Auto: Off';
        btn.classList.remove('btn-active');
    }
}

function toggleAutoRefresh() {
    if (autoRefreshInterval) {
        stopAutoRefresh();
    } else {
        startAutoRefresh(30);
    }
}

// Initialize ApexCharts with 8-bit pixel art theme
function initializeCharts() {
    const tempOptions = {
        series: [{
            name: 'Temperature',
            data: []
        }],
        chart: {
            type: 'line',
            height: 280,
            background: '#fff',
            foreColor: '#000',
            fontFamily: 'ModernDOS, Courier New, monospace',
            zoom: {
                enabled: true,
                type: 'x'
            },
            toolbar: {
                show: true,
                tools: {
                    download: false
                }
            },
            animations: {
                enabled: false
            },
            events: {
                zoomed: function(chartContext, { xaxis }) {
                    syncCharts(xaxis);
                },
                scrolled: function(chartContext, { xaxis }) {
                    syncCharts(xaxis);
                }
            }
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            curve: 'stepline',
            width: 2
        },
        xaxis: {
            type: 'datetime',
            labels: {
                datetimeUTC: false,
                style: {
                    colors: '#000',
                    fontFamily: 'ModernDOS, monospace',
                    fontSize: '14px'
                }
            },
            axisBorder: {
                color: '#000',
                strokeWidth: 3
            },
            axisTicks: {
                color: '#000'
            }
        },
        yaxis: {
            labels: {
                formatter: function(val) {
                    return val.toFixed(1) + '°';
                },
                style: {
                    colors: '#000',
                    fontFamily: 'ModernDOS, monospace',
                    fontSize: '14px'
                }
            }
        },
        tooltip: {
            x: {
                format: 'dd MMM yyyy HH:mm:ss'
            },
            y: {
                formatter: function(val) {
                    return val.toFixed(2) + ' °C';
                }
            },
            theme: 'light',
            style: {
                fontFamily: 'ModernDOS, monospace',
                fontSize: '14px'
            }
        },
        colors: ['#000'],
        grid: {
            borderColor: '#ccc',
            strokeDashArray: 0
        },
        markers: {
            size: 5,
            colors: ['#000'],
            strokeWidth: 0,
            shape: 'square'
        }
    };

    const humOptions = {
        series: [{
            name: 'Humidity',
            data: []
        }],
        chart: {
            type: 'line',
            height: 280,
            background: '#fff',
            foreColor: '#000',
            fontFamily: 'ModernDOS, Courier New, monospace',
            zoom: {
                enabled: true,
                type: 'x'
            },
            toolbar: {
                show: true,
                tools: {
                    download: false
                }
            },
            animations: {
                enabled: false
            },
            events: {
                zoomed: function(chartContext, { xaxis }) {
                    syncCharts(xaxis);
                },
                scrolled: function(chartContext, { xaxis }) {
                    syncCharts(xaxis);
                }
            }
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            curve: 'stepline',
            width: 2
        },
        xaxis: {
            type: 'datetime',
            labels: {
                datetimeUTC: false,
                style: {
                    colors: '#000',
                    fontFamily: 'ModernDOS, monospace',
                    fontSize: '14px'
                }
            },
            axisBorder: {
                color: '#000',
                strokeWidth: 3
            },
            axisTicks: {
                color: '#000'
            }
        },
        yaxis: {
            min: 0,
            max: 100,
            labels: {
                formatter: function(val) {
                    return val.toFixed(0) + '%';
                },
                style: {
                    colors: '#000',
                    fontFamily: 'ModernDOS, monospace',
                    fontSize: '14px'
                }
            }
        },
        tooltip: {
            x: {
                format: 'dd MMM yyyy HH:mm:ss'
            },
            y: {
                formatter: function(val) {
                    return val.toFixed(2) + ' %RH';
                }
            },
            theme: 'light',
            style: {
                fontFamily: 'ModernDOS, monospace',
                fontSize: '14px'
            }
        },
        colors: ['#000'],
        grid: {
            borderColor: '#ccc',
            strokeDashArray: 0
        },
        markers: {
            size: 5,
            colors: ['#000'],
            strokeWidth: 0,
            shape: 'square'
        }
    };

    tempCharts = [];
    humCharts = [];
    for (let i = 0; i < 4; i++) {
        const te = document.querySelector('#tempChart' + i);
        const he = document.querySelector('#humChart' + i);
        if (te) {
            const c = new ApexCharts(te, JSON.parse(JSON.stringify(tempOptions)));
            c.render();
            tempCharts.push(c);
        } else {
            tempCharts.push(null);
        }
        if (he) {
            const c = new ApexCharts(he, JSON.parse(JSON.stringify(humOptions)));
            c.render();
            humCharts.push(c);
        } else {
            humCharts.push(null);
        }
    }
}

// Sync all 8 charts when one is zoomed/panned
function syncCharts(xaxis) {
    if (!xaxis || !xaxis.min || !xaxis.max) return;
    const all = [...tempCharts, ...humCharts].filter(Boolean);
    all.forEach(function(ch) {
        try {
            ch.zoomX(xaxis.min, xaxis.max);
        } catch (e) {}
    });
}

// Format sampling interval for display (e.g. "10 s", "1 h")
function formatSampleInterval(seconds) {
    if (!seconds || seconds < 0) return '--';
    if (seconds < 60) return seconds + ' s';
    if (seconds < 3600) return (seconds / 60) + ' min';
    if (seconds < 86400) return (seconds / 3600) + ' h';
    return (seconds / 86400) + ' d';
}

// Derive duration/interval from legacy mode string
function heatingModeToDurationInterval(mode) {
    if (mode === '10s_5min') return { duration: 10, interval: 300 };
    if (mode === '1min_1hr') return { duration: 60, interval: 3600 };
    if (mode === '1min_1day') return { duration: 60, interval: 86400 };
    return { duration: 0, interval: 0 };
}

// Ensure per-sensor arrays exist (backend may return sample_period_sensor, heating_mode_sensor, heating_duration_sensor, heating_interval_sensor)
function ensurePerSensorConfig() {
    if (!Array.isArray(deviceConfig.sample_period_sensor)) {
        const single = deviceConfig.sample_period_s || 3600;
        deviceConfig.sample_period_sensor = [single, single, single, single];
    }
    if (!Array.isArray(deviceConfig.heating_mode_sensor)) {
        const mode = deviceConfig.heating_mode || 'off';
        deviceConfig.heating_mode_sensor = [mode, 'off', 'off', 'off'];
    }
    if (!Array.isArray(deviceConfig.heating_duration_sensor) || !Array.isArray(deviceConfig.heating_interval_sensor)) {
        deviceConfig.heating_duration_sensor = [0, 0, 0, 0];
        deviceConfig.heating_interval_sensor = [0, 0, 0, 0];
        for (let i = 0; i < 4; i++) {
            const m = deviceConfig.heating_mode_sensor[i] || 'off';
            const t = heatingModeToDurationInterval(m);
            deviceConfig.heating_duration_sensor[i] = t.duration;
            deviceConfig.heating_interval_sensor[i] = t.interval;
        }
    }
}

const SAMPLING_OPTIONS = [10, 30, 60, 300, 600, 3600, 21600, 86400];

function closestSamplingOption(sec) {
    if (SAMPLING_OPTIONS.indexOf(sec) >= 0) return sec;
    let best = 3600;
    for (const o of SAMPLING_OPTIONS) {
        if (o <= sec) best = o;
        else break;
    }
    return best;
}

// Enable/disable duty cycle input based on interval selection
function applyHeatingCycleConstraint(index) {
    const intervalEl = document.getElementById('sensor' + index + 'HeatingInterval');
    const dutyCycleEl = document.getElementById('sensor' + index + 'HeatingDutyCycle');
    if (!intervalEl || !dutyCycleEl) return;
    const interval = parseInt(intervalEl.value, 10) || 0;
    dutyCycleEl.disabled = interval === 0;
    if (interval === 0) {
        dutyCycleEl.value = 0;
    } else {
        let pct = parseInt(dutyCycleEl.value, 10) || 0;
        if (pct < 0) pct = 0;
        if (pct > 100) pct = 100;
        dutyCycleEl.value = pct;
    }
}

// Update all sensor option rows (per-sensor sampling + heating interval/duty cycle)
function updateAllSensorRowSettings() {
    ensurePerSensorConfig();
    const intervalOptions = [0, 300, 3600, 86400];
    for (let i = 0; i < 4; i++) {
        const sel = document.getElementById('sensor' + i + 'Sampling');
        if (sel) {
            let sec = deviceConfig.sample_period_sensor[i] || 3600;
            sec = closestSamplingOption(sec);
            sel.value = String(sec);
        }
        const intervalEl = document.getElementById('sensor' + i + 'HeatingInterval');
        const dutyCycleEl = document.getElementById('sensor' + i + 'HeatingDutyCycle');
        if (intervalEl) {
            let inv = deviceConfig.heating_interval_sensor[i] || 0;
            intervalEl.value = intervalOptions.indexOf(inv) >= 0 ? String(inv) : '0';
        }
        if (dutyCycleEl) {
            const dur = deviceConfig.heating_duration_sensor[i] || 0;
            const inv = deviceConfig.heating_interval_sensor[i] || 0;
            const pct = (inv > 0) ? Math.round(dur / inv * 100) : 0;
            dutyCycleEl.value = pct;
        }
        applyHeatingCycleConstraint(i);
    }
}

// Load configuration
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        if (response.ok) {
            deviceConfig = await response.json();
            document.getElementById('deviceId').textContent = deviceConfig.device_id || 'ESP8266 Logger';
            updateSettingsForm();
            updateAllSensorRowSettings();
        }
    } catch (error) {
        console.error('Failed to load config:', error);
    }
}

// Update settings form with current values (interval only)
function updateSettingsForm() {
    if (!deviceConfig.sample_period_s) return;
    
    const period = deviceConfig.sample_period_s;
    let value, unit;
    
    if (period < 3600) {
        if (period >= 60 && period % 60 === 0) {
            value = period / 60;
            unit = 'minutes';
        } else {
        value = period;
        unit = 'seconds';
        }
    } else if (period < 86400) {
        value = period / 3600;
        unit = 'hours';
    } else {
        value = period / 86400;
        unit = 'days';
    }
    
    document.getElementById('intervalValue').value = value;
    document.getElementById('intervalUnit').value = unit;
    updateIntervalConstraints();
}

function onModalHeatingIntervalChange() {
    const intervalEl = document.getElementById('modalHeatingInterval0');
    const dutyCycleEl = document.getElementById('modalHeatingDutyCycle0');
    if (!intervalEl || !dutyCycleEl) return;
    const interval = parseInt(intervalEl.value, 10) || 0;
    dutyCycleEl.disabled = interval === 0;
    if (interval === 0) dutyCycleEl.value = 0;
    else {
        let pct = parseInt(dutyCycleEl.value, 10) || 0;
        if (pct < 0) pct = 0;
        if (pct > 100) pct = 100;
        dutyCycleEl.value = pct;
    }
}

// Update sensors form with current values (modal: sensor 0 heating)
function updateSensorsForm() {
    ensurePerSensorConfig();
    const intervalOptions = [0, 300, 3600, 86400];
    const intervalEl = document.getElementById('modalHeatingInterval0');
    const dutyCycleEl = document.getElementById('modalHeatingDutyCycle0');
    if (intervalEl) {
        const inv = deviceConfig.heating_interval_sensor[0] || 0;
        intervalEl.value = intervalOptions.indexOf(inv) >= 0 ? String(inv) : '0';
    }
    if (dutyCycleEl) {
        const dur = deviceConfig.heating_duration_sensor[0] || 0;
        const inv = deviceConfig.heating_interval_sensor[0] || 0;
        const pct = (inv > 0) ? Math.round(dur / inv * 100) : 0;
        dutyCycleEl.value = pct;
        dutyCycleEl.disabled = inv === 0;
    }
}

function updateIntervalConstraints() {
    const valueInput = document.getElementById('intervalValue');
    const unitSelect = document.getElementById('intervalUnit');
    if (!valueInput || !unitSelect) return;

    const unit = unitSelect.value;
    valueInput.min = unit === 'seconds' ? 10 : 1;
}

// Load storage status
async function loadStorageStatus() {
    try {
        const response = await fetch('/api/storage');
        if (response.ok) {
            const data = await response.json();
            const lfs = data.lfs;
            const sd = data.sd;

            let statusText = `LFS: ${formatBytes(lfs.free)} free`;
            if (sd.present) {
                statusText += ` | SD: OK`;
            }
            
            document.getElementById('storageStatus').textContent = statusText;

            const capacityEl = document.getElementById('capacityStatus');
        if (capacityEl) {
            if (data.retention) {
                const estSamples = Number.isFinite(data.retention.est_samples) ? data.retention.est_samples : 0;
                const estDuration = Number.isFinite(data.retention.est_duration_s) ? data.retention.est_duration_s : 0;
                capacityEl.textContent = `Capacity: ${formatCount(estSamples)} samples / ${formatDuration(estDuration)}`;
            } else {
                capacityEl.textContent = 'Capacity: unavailable';
            }
        }
            
            if (data.write_errors > 0) {
                document.getElementById('storageStatus').classList.add('status-error');
                document.getElementById('storageStatus').textContent += ` (${data.write_errors} errors)`;
            }
        }
    } catch (error) {
        console.error('Failed to load storage status:', error);
    }
}

// Format bytes
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function formatCount(count) {
    if (count >= 1000000) return (count / 1000000).toFixed(1) + 'M';
    if (count >= 1000) return (count / 1000).toFixed(1) + 'k';
    return String(count);
}

function formatDuration(seconds) {
    if (!seconds || seconds <= 0) return '0';
    const days = seconds / 86400;
    if (days >= 1) return days.toFixed(1) + ' days';
    const hours = seconds / 3600;
    if (hours >= 1) return hours.toFixed(1) + ' hours';
    const minutes = Math.max(1, Math.round(seconds / 60));
    return minutes + ' min';
}

async function pruneLfs() {
    const input = document.getElementById('pruneDays');
    const btn = document.getElementById('pruneBtn');
    if (!input || !btn) return;

    const days = parseInt(input.value, 10);
    if (!Number.isFinite(days) || days <= 0) {
        alert('Please enter a valid number of days');
        return;
    }

    if (!confirm(`Delete LittleFS data older than ${days} days?`)) {
        return;
    }

    btn.disabled = true;
    const oldLabel = btn.textContent;
    btn.textContent = 'Pruning...';

    try {
        const response = await fetch(`/api/prune?days=${encodeURIComponent(days)}`);
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Prune failed');
        }

        alert(`Deleted ${data.deleted_samples} samples, freed ${formatBytes(data.freed_bytes)}`);
        loadStorageStatus();
        loadData(true);
    } catch (error) {
        console.error('Failed to prune LFS:', error);
        alert('Failed to prune: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = oldLabel;
    }
}

// Check time status
async function checkTimeStatus() {
    try {
        const response = await fetch('/api/time');
        if (response.ok) {
            const data = await response.json();
            const timeStatus = document.getElementById('timeStatus');
            
            if (data.time_set) {
                // Time will be updated by updateLiveStatus
                timeStatus.className = 'status-badge status-ok';
                document.getElementById('timeModal').classList.remove('show');
            } else {
                timeStatus.textContent = 'Time: Not set!';
                timeStatus.className = 'status-badge status-warning';
                document.getElementById('timeModal').classList.add('show');
            }
        }
    } catch (error) {
        console.error('Failed to check time status:', error);
    }
}

// Set device time from browser
async function setDeviceTime() {
    const errorDiv = document.getElementById('timeError');
    errorDiv.classList.remove('show');
    
    try {
        const epoch = Math.floor(Date.now() / 1000);
        const response = await fetch('/api/time', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ epoch: epoch })
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            document.getElementById('timeModal').classList.remove('show');
            checkTimeStatus();
        } else {
            errorDiv.textContent = data.error || 'Failed to set time';
            errorDiv.classList.add('show');
        }
    } catch (error) {
        errorDiv.textContent = 'Network error';
        errorDiv.classList.add('show');
    }
}

// Set quick time range
function setQuickRange(days, silent = false) {
    const to = new Date();
    const from = new Date();
    from.setDate(from.getDate() - days);

    document.getElementById('fromDate').value = formatDateTimeLocal(from);
    document.getElementById('toDate').value = formatDateTimeLocal(to);

    // Load immediately after changing the range for faster feedback
    loadData(silent);
}

// Format date for datetime-local input
function formatDateTimeLocal(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

// Format date to ISO8601
function formatISO8601(date) {
    return date.toISOString().split('.')[0] + 'Z';
}

// Load data (silent = no alerts)
async function loadData(silent = false) {
    const fromInput = document.getElementById('fromDate').value;
    const toInput = document.getElementById('toDate').value;
    
    if (!fromInput || !toInput) {
        if (!silent) alert('Please select both from and to dates');
        return;
    }
    
    const from = new Date(fromInput);
    const to = new Date(toInput);
    
    if (from >= to) {
        if (!silent) alert('From date must be before to date');
        return;
    }
    
    try {
        const fromISO = formatISO8601(from);
        const toISO = formatISO8601(to);
        
        const response = await fetch(`/api/data?from=${encodeURIComponent(fromISO)}&to=${encodeURIComponent(toISO)}`);
        
        if (!response.ok) {
            throw new Error('Failed to load data');
        }
        
        const data = await response.json();
        
        if (data.warning && !silent) {
            alert(data.warning);
        }
        
        const tempDataBySensor = [[], [], [], []];
        const humDataBySensor = [[], [], [], []];
        
        if (data.points && Array.isArray(data.points)) {
            for (let i = 0; i < data.points.length; i++) {
                const point = data.points[i];
                if (!point || point.length < 3) continue;
                const timestamp = new Date(point[0]).getTime();
                const numVal = point.length >= 9 ? 4 : 1;
                for (let s = 0; s < numVal; s++) {
                    const ti = s * 2 + 1;
                    const hi = s * 2 + 2;
                    const tv = point[ti];
                    const hv = point[hi];
                    if (tv != null && hv != null && tv !== 'null' && hv !== 'null') {
                        const tn = Number(tv);
                        const hn = Number(hv);
                        if (Number.isFinite(tn) && Number.isFinite(hn)) {
                            tempDataBySensor[s].push([timestamp, tn]);
                            humDataBySensor[s].push([timestamp, hn]);
                        }
                    }
                }
            }
        } else if (data.ts && data.temp && data.hum) {
            for (let i = 0; i < data.ts.length; i++) {
                const timestamp = new Date(data.ts[i]).getTime();
                tempDataBySensor[0].push([timestamp, data.temp[i]]);
                humDataBySensor[0].push([timestamp, data.hum[i]]);
            }
        }
        let firstTs = null;
        let lastTs = null;
        if (tempDataBySensor[0].length > 0) {
            firstTs = new Date(tempDataBySensor[0][0][0]).toISOString();
            lastTs = new Date(tempDataBySensor[0][tempDataBySensor[0].length - 1][0]).toISOString();
        }
        
        for (let s = 0; s < 4; s++) {
            if (tempCharts[s]) {
                tempCharts[s].updateSeries([{ name: 'Temperature', data: tempDataBySensor[s] }]);
            }
            if (humCharts[s]) {
                humCharts[s].updateSeries([{ name: 'Humidity', data: humDataBySensor[s] }]);
            }
        }
        
        const countEl = document.getElementById('dataCount');
        if (countEl) {
            const count = Number.isFinite(data.count) ? data.count : (data.points && data.points.length) || 0;
            countEl.textContent = `${count} points`;
        }
        
    } catch (error) {
        console.error('Failed to load data:', error);
        if (!silent) alert('Failed to load data: ' + error.message);
    }
}

// Download CSV. If sensorIndex is 0..3, fetch and save only that sensor's columns; otherwise full CSV via navigation.
async function downloadCSV(sensorIndex) {
    const fromInput = document.getElementById('fromDate').value;
    const toInput = document.getElementById('toDate').value;
    
    if (!fromInput || !toInput) {
        alert('Please select both from and to dates');
        return;
    }
    
    const from = new Date(fromInput);
    const to = new Date(toInput);
    
    if (from >= to) {
        alert('From date must be before to date');
        return;
    }
    
    const fromISO = formatISO8601(from);
    const toISO = formatISO8601(to);
    const url = `/api/download?from=${encodeURIComponent(fromISO)}&to=${encodeURIComponent(toISO)}&format=csv&store=auto`;
    
    if (sensorIndex === undefined || sensorIndex === null) {
        window.location.href = url;
        return;
    }
    
    const idx = parseInt(sensorIndex, 10);
    if (idx < 0 || idx > 3) {
        window.location.href = url;
        return;
    }
    
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Download failed');
        const fullCsv = await response.text();
        const lines = fullCsv.split(/\r?\n/);
        if (lines.length === 0) {
            alert('No data in range');
            return;
        }
        const header = lines[0];
        const isSparse = header.indexOf('sensor_id') >= 0;
        let outLines;
        if (isSparse) {
            outLines = ['timestamp,t_c,h_rh'];
            for (let i = 1; i < lines.length; i++) {
                const row = lines[i].split(',');
                if (row.length >= 4 && parseInt(row[1], 10) === idx) {
                    outLines.push([row[0], row[2], row[3]].join(','));
                }
            }
        } else {
            const cols = header.split(',');
            const wantCols = [0, 1, 2 + idx * 2, 3 + idx * 2];
            const sensorHeader = wantCols.map(c => cols[c]).join(',');
            outLines = [sensorHeader];
            for (let i = 1; i < lines.length; i++) {
                const row = lines[i].split(',');
                if (row.length >= 4 + idx * 2) {
                    outLines.push(wantCols.map(c => row[c]).join(','));
                }
            }
        }
        const blob = new Blob([outLines.join('\n')], { type: 'text/csv' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = (deviceConfig.device_id || 'logger') + '_' + fromISO.substring(0, 10) + '_' + toISO.substring(0, 10) + '_sensor' + idx + '.csv';
        a.click();
        URL.revokeObjectURL(a.href);
    } catch (error) {
        console.error('Failed to download CSV:', error);
        alert('Failed to download CSV: ' + error.message);
    }
}

// Settings modal
function openSettings() {
    updateSettingsForm();
    document.getElementById('settingsModal').classList.add('show');
}

function closeSettings() {
    document.getElementById('settingsModal').classList.remove('show');
    document.getElementById('settingsError').classList.remove('show');
}

// Save one sensor's sampling rate (onchange); sends full sample_period_sensor array
async function saveSensorSampling(index) {
    ensurePerSensorConfig();
    const el = document.getElementById('sensor' + index + 'Sampling');
    if (!el) return;
    const sec = parseInt(el.value, 10);
    if (isNaN(sec) || sec < 10 || sec > 604800) return;
    const arr = deviceConfig.sample_period_sensor.slice();
    arr[index] = sec;
    // Prefer WebSocket for instant UI feel; fallback to HTTP
    if (wsSend({ type: 'set', sample_period_sensor: arr })) {
        deviceConfig.sample_period_sensor = arr;
        deviceConfig.sample_period_s = arr[0];
        return;
    }
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sample_period_sensor: arr })
        });
        const data = await response.json();
        if (response.ok && data.success) {
            deviceConfig.sample_period_sensor = arr;
            deviceConfig.sample_period_s = arr[0];
            updateLiveStatus();
        }
    } catch (error) {
        console.error('Failed to save sensor sampling:', error);
    }
}

// When heating interval changes: update constraint and save (duty cycle % is preserved)
function onHeatingIntervalChange(index) {
    applyHeatingCycleConstraint(index);
    saveSensorHeating(index);
}

// Save one sensor's heating (duty cycle % + interval); computes duration from pct and sends to API
async function saveSensorHeating(index) {
    ensurePerSensorConfig();
    const intervalEl = document.getElementById('sensor' + index + 'HeatingInterval');
    const dutyCycleEl = document.getElementById('sensor' + index + 'HeatingDutyCycle');
    if (!intervalEl || !dutyCycleEl) return;
    let interval = parseInt(intervalEl.value, 10) || 0;
    let pct = parseInt(dutyCycleEl.value, 10) || 0;
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    let duration = (interval > 0) ? Math.round(interval * pct / 100) : 0;
    if (interval === 0) duration = 0;
    const durationArr = deviceConfig.heating_duration_sensor.slice();
    const intervalArr = deviceConfig.heating_interval_sensor.slice();
    durationArr[index] = duration;
    intervalArr[index] = interval;
    // Prefer WebSocket for instant UI feel; fallback to HTTP
    if (wsSend({ type: 'set', heating_duration_sensor: durationArr, heating_interval_sensor: intervalArr })) {
        deviceConfig.heating_duration_sensor = durationArr;
        deviceConfig.heating_interval_sensor = intervalArr;
        return;
    }
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                heating_duration_sensor: durationArr,
                heating_interval_sensor: intervalArr
            })
        });
        const data = await response.json();
        if (response.ok && data.success) {
            deviceConfig.heating_duration_sensor = durationArr;
            deviceConfig.heating_interval_sensor = intervalArr;
            updateLiveStatus();
        }
    } catch (error) {
        console.error('Failed to save sensor heating:', error);
    }
}

// Sensors modal
function openSensors() {
    updateSensorsForm();
    document.getElementById('sensorsModal').classList.add('show');
}

function closeSensors() {
    document.getElementById('sensorsModal').classList.remove('show');
    document.getElementById('sensorsError').classList.remove('show');
}

async function saveSensors(event) {
    event.preventDefault();
    const errorDiv = document.getElementById('sensorsError');
    errorDiv.classList.remove('show');
    ensurePerSensorConfig();
    const intervalEl = document.getElementById('modalHeatingInterval0');
    const dutyCycleEl = document.getElementById('modalHeatingDutyCycle0');
    let interval = parseInt(intervalEl && intervalEl.value ? intervalEl.value : 0, 10) || 0;
    let pct = parseInt(dutyCycleEl && dutyCycleEl.value ? dutyCycleEl.value : 0, 10) || 0;
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    let duration = (interval > 0) ? Math.round(interval * pct / 100) : 0;
    if (interval === 0) duration = 0;
    const durationArr = deviceConfig.heating_duration_sensor.slice();
    const intervalArr = deviceConfig.heating_interval_sensor.slice();
    durationArr[0] = duration;
    intervalArr[0] = interval;
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                heating_duration_sensor: durationArr,
                heating_interval_sensor: intervalArr
            })
        });
        const data = await response.json();
        if (response.ok && data.success) {
            closeSensors();
            loadConfig();
            updateLiveStatus();
        } else {
            errorDiv.textContent = data.error || 'Failed to save sensor settings';
            errorDiv.classList.add('show');
        }
    } catch (error) {
        errorDiv.textContent = 'Network error';
        errorDiv.classList.add('show');
    }
}

async function saveSettings(event) {
    event.preventDefault();
    const errorDiv = document.getElementById('settingsError');
    errorDiv.classList.remove('show');
    
    const value = parseFloat(document.getElementById('intervalValue').value);
    const unit = document.getElementById('intervalUnit').value;
    
    if (isNaN(value) || value <= 0) {
        errorDiv.textContent = 'Invalid interval value';
        errorDiv.classList.add('show');
        return;
    }

    if (unit === 'seconds' && value < 10) {
        errorDiv.textContent = 'Seconds must be at least 10';
        errorDiv.classList.add('show');
        return;
    }
    
    let periodSeconds;
    if (unit === 'seconds') {
        periodSeconds = Math.round(value);
    } else if (unit === 'minutes') {
        periodSeconds = Math.round(value * 60);
    } else if (unit === 'hours') {
        periodSeconds = Math.round(value * 3600);
    } else if (unit === 'days') {
        periodSeconds = Math.round(value * 86400);
    }
    
    if (periodSeconds < 10 || periodSeconds > 604800) {
        errorDiv.textContent = 'Interval must be between 10 seconds and 7 days';
        errorDiv.classList.add('show');
        return;
    }
    
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ sample_period_s: periodSeconds })
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            closeSettings();
            loadConfig();
            updateLiveStatus();
            loadStorageStatus();
        } else {
            errorDiv.textContent = data.error || 'Failed to save settings';
            errorDiv.classList.add('show');
        }
    } catch (error) {
        errorDiv.textContent = 'Network error';
        errorDiv.classList.add('show');
    }
}

// Test SD card
async function testSD() {
    try {
        const response = await fetch('/api/test-sd');
        const data = await response.json();
        
        if (data.success) {
            alert('✓ SD Card Test Passed!\n\n' + data.message);
        } else {
            alert('✗ SD Card Test Failed!\n\n' + data.error);
        }
    } catch (error) {
        alert('✗ SD Card Test Failed!\n\nNetwork error: ' + error.message);
    }
}
